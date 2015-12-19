#> !P3!

#> 1v0
#> 1v1 - DOSQUAWK, Quiet KBI, XLOG:xlog, XLOG.heartbeats, 
#        new ffwdb.py and simpler watching logic.
#> 1v2 - fix thread stop 

###
### xlog2db:
###
###     A file watcher of xlog output flatfiles.
###     Normal logfile messages are loaded to table XLOG:xlog.
###     Heartbeat logrecs are stored in table XLOG.heartbeats for 
###       monitoring.  Only the lastest heartbeats are stored.
###     Files are named "YYMMDD-HH.log" for natural sequencing.
###       No file inodes or timestamps are used.
###     The newest file in the watched folder is "live", while 
###       any older ones are static history. After history files 
###       are loaded, they are moved to a DONESD subdirectory.
###
###     Windows and Linux compatible.
###     Historical files will be reprocessed in their entirety 
###       if terminated early. SHA1 hashes allows for the 
###       skipping of duplicate logrecs during reruns.
###     Rerun heartbeat records are harmless because only newer
###       timestamps are acknowledged.
###
###     OLD flatfile format: used '|' to delimit fields in each 
###       record's prefix.  There was no version indicator.
###     NEW flatfile format: tne first field in the prefix is '1'
###       and prefix fields are tab delimited. "_si" (client 
###       sub-id) has been added to the logrec dict.
###

"""
Usage:
  xlog2db.py [--ini=<ini> --srcid=<srcid> --subid=<subid> --wpath=<wpath> --donesd=<donesd> --interval=<interval> --xlogdb=<xlogdb>]
  xlog2db.py (-h | --help)
  xlog2db.py --version

Options:
  -h --help              Show help.
  --version              Show version.
  --ini=<ini>            Overrides default ini pfn.
  --srcid=<srcid>        Source ID ("xlog").
  --subid=<subid>        Sub    ID ("2db_").
  --wpath=<wpath>        Path to be watched for "yymmdd-hh.log" pattern.
  --donesd=<donesd>      Subdir of wpath for done files. Null disables.
  --interval=<interval>  Interval (seconds).
  --xlogdb=<xlogdb>      CFG of xlog database.
"""

import os, sys, stat
import time, datetime, calendar
import shutil
import collections
import pickle
import copy
import json
import threading
import re
import gzip
import hashlib

gP2 = (sys.version_info[0] == 2)
gP3 = (sys.version_info[0] == 3)
assert gP3, 'requires Python 3'

gWIN = sys.platform.startswith('win')
gLIN = sys.platform.startswith('lin')

####################################################################################################

# Setup.

WPATH = None                        # Everything is here, until it's sent.
DONESD = None                       # Then it's here, if not None.                     
INTERVAL = 6                        # Seconds.
ENCODING = 'utf-8'                 
ERRORS = 'strict'

import l_args as _a
ME = _a.get_args(__doc__, '0.1')

import l_screen_writer
_sw = l_screen_writer.ScreenWriter()
import l_simple_logger
_sl = l_simple_logger.SimpleLogger(screen_writer=_sw)

import l_dt as _dt
import l_misc as _m

OWNHEARTBEAT = True                 # Add our own heartbeat records to XLOG:heartbeats.
SRCID = SUBID = None                # SRCID, SUBID for our own heartbeats.

TEST = False                        # Unused.
TESTONLY = False                    # Unused.
ONECHECK = False		            # Once around watcher_thread loop.
TIMINGS = False                     # Timing in watcher_thread loop.
TRACINGS = False                    # Extra details
NOLOAD = False                      # NOP during testing.

# A logging file?
LFPFN = 'LOG.txt'                   # May or may not stay.
if LFPFN:
    LF = open(LFPFN, 'a', encoding=ENCODING, errors=ERRORS)
else:
    LF = None

####################################################################################################

SQUAWKED = False                # To suppress chained exception messages.
def DOSQUAWK(errmsg, beeps=3):
    """For use in exception blocks."""
    global SQUAWKED
    if not SQUAWKED:
        _m.beeps(beeps)
        for em in errmsg.split('\n'):
            _sl.error(em)
        SQUAWKED = True

####################################################################################################

# >>> Table [xlog].  

DB_FNS_XLOG = ('id', 'rxts', 'txts', 'srcid', 'subid', 'el', 'sl', 'sha1', 'kvs')
DB_FNL_XLOG = DB_FIL_XLOG = ''  # Field name list, %s insertion list.
for fn in DB_FNS_XLOG:
    if fn == 'id':              # Skip the ISN.
        continue
    if DB_FNL_XLOG:
        DB_FNL_XLOG += ', '
        DB_FIL_XLOG += ', '
    DB_FNL_XLOG += fn
    DB_FIL_XLOG += '%s'

# >>> Table [heartbeats].  (The same fields as in the xlog table.)
DB_FNS_HBS = DB_FNS_XLOG
DB_FNL_HBS, DB_FIL_HBS = DB_FNL_XLOG, DB_FIL_XLOG
DB_FUL_HBS = ''                 # Field update list.
for fn in DB_FNS_XLOG:
    if fn == 'id':              # Skip the ISN.
        continue
    if DB_FUL_HBS:
        DB_FUL_HBS += ', '
    DB_FUL_HBS += '%s=%s' % (fn, '%s')
DB_FUL_HBS = DB_FUL_HBS

XLOGDB = None                   # The db connection.
LOADRECS = None                 # Batches db loadrecs (really tuples) (created from logrecs).
LOADCOMMITBATCHSIZE = 1000      # Inter-commit load count.

NDUPE = NNEW = 0

import mysql.connector as mc

####################################################################################################

# Trim whitespace.  Remove trailing comma.  Remove quotes.  '-', ' ', '' -> None.
def _S(x):
    if   isinstance(x, bytes):
        return x.decode(encoding=ENCODING, errors=ERRORS)
    elif x is None:
        return None
    elif isinstance(x, bool):
        return '1' if x else '0'
    else:
        return str(x)

####################################################################################################

# Database of log files in watched directory: xlog2db.s3:
#   Table logfiles: ('filename', 'ymd', 'hh', 'modified', 'size', 'acquired', 'processed')
# Note: sqlite3 db must be opened in watcherThread.

FFWDBPFN = FFWDB = None
import ffwdb

####################################################################################################

# Filename pattern: yymmdd-hh.log

FNPATTERN = r'\d{6}-\d{2}\.log'    # or '\d{6}-\d{2}\.[lL][oO][gG]' or some other case insensitivity
REFNPATTERN = re.compile(FNPATTERN)

####################################################################################################

def shutDown():
    FFWDB.disconnect()
    try:    XLOGDB.close()
    except: pass

#
# updateDB: Update or add to FFWDB, given a file info dict.
#
def updateDB(ufi):
    """Update FFWDB from given file info dict.  Returns dbfi."""
    me = 'updateDB'
    dbfi = None
    try:
        dbfi = FFWDB.select(ufi['filename'])
        # Insert?
        if not dbfi:
            z = copy.copy(ufi)
            z['processed'] = 0
            dbfi = FFWDB.insert(z)                  # Returns inserted.
            z = None
            if not dbfi:
                raise ValueError('db insertion failed')
        # Update:
        else:
            if dbfi['modified'] != ufi['modified'] or \
               dbfi['size'] != ufi['size']:
                z = {}
                z['filename'] = ufi['filename']
                z['modified'] = ufi['modified']
                z['size'] = ufi['size']
                z['acquired'] = ufi['acquired']
                dbfi = FFWDB.update(z)              # Returns updated.     
                assert dbfi, 'no dbfi returned from update'
                z = None
            else:
                pass
            pass
        pass
    except Exception as E:
        dbfi = None
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        return dbfi

def testS2E(s2e):
    if not (TEST and s2e):
        return
    ###!!!
    return
    ###!!!
    me = 'testS2E'
    _sl.info()
    _sl.info('{:s}:  {:,d} bytes'.format(me, len(s2e)))
    ne = 0
    try:
        if s2e[-1] == '\n':
            _sl.info('ends with \\n')
        else:
            _sl.info('does not end with \\n')
        for x, logrec in enumerate(s2e.split('\n')):
            if not logrec:
                continue
            pass
        pass
    except Exception as E:
        rc = False
        errmsg = '%s: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        pass
    finally:
        return (ne == 0)

HB_EL, HB_SL = '0', 'h'         # !MAGIC! Heartbeat error level, sub level.
HEARTBEATS = {}                 # Staging dict for heartbeat records.
NBEATS = NOLDBEATS = 0

#
# addHeartbeat: Add a heartbeat to a staging dict.
#               Only new or newer (by txts) are added.
#   1~rxts~txts~srcid~subid~el~sl~sha1~kvs       
#   {"_el": "0", "_id": "SRC_", "_ip": "192.168.100.6", "_si": "SUB_", "_sl": "h", "_ts": "1449937097.9118", "dt_loc": "2015-12-12 08:18:17.9118", "dt_utc": "2015-12-12 16:18:17.9118"}
#
def addHeartbeat(logrec):
    global HEARTBEATS, NBEATS, NOLDBEATS
    me = 'addHeartbeat(%s)' % repr(logrec)
    try:
        # Unpack logrec.
        (fv, rxts, txts, srcid, subid, el, sl, sha1, kvs) = logrec2fields(logrec)
        if fv is None:
            return
        assert ((el == HB_EL) and (sl == HB_SL)), 'bad hb el (%s) or sl (%s)' % (repr(el), repr(sl))
        assert txts, 'hb needs a txts'
        NBEATS += 1
        rxts2, txts2 = float(rxts), float(txts)
        # Check staging dict.
        k = srcid + '|' + subid
        v = HEARTBEATS.get(k)
        if v:
            if not (txts2 > float(v[1])):       # !MAGIC! Tuple index.
                NOLDBEATS += 1
                return 
        # logrec is new or newer: update staging dict. 
        v = [_S(rxts2), _S(txts2), _S(srcid), _S(subid), _S(el), _S(sl), _S(sha1), _S(kvs)]  # !!! Matches xlog/heartbeats table.
        HEARTBEATS[k] = v
    except Exception as E:
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        pass

#
# flushHeartbeats: Load staging dict contents into XLOG.heartbeats.
#                  Only new or newer (by txts) are loaded.
#
def flushHeartbeats():
    global HEARTBEATS
    me = 'flushHeartbeats'
    try:
        if not HEARTBEATS:
            return
        assert XLOGDB, 'no XLOGDB'
        for k, v in HEARTBEATS.items():
            txts, srcid, subid = float(v[1]), v[2], v[3]    # !MAGIC! tuple indices.
            try:
                c = XLOGDB.cursor()
                c.execute('select txts from heartbeats where srcid=%s and subid=%s', (srcid, subid))
                try:    xtxts = c.fetchone()[0]
                except: xtxts = None
            finally:
                c.close()
            try:
                c = XLOGDB.cursor()
                if   xtxts is None:
                    sql = 'insert into heartbeats (%s) values (%s)' % (DB_FNL_HBS, DB_FIL_HBS)
                    c.execute(sql, v)
                elif txts > xtxts:
                    sql = 'update heartbeats set %s where srcid=%s and subid=%s' % (DB_FUL_HBS, '%s', '%s')
                    v.append(srcid)
                    v.append(subid)
                    c.execute(sql, v)
                else:
                    continue
            finally:
                c.close()
        pass
    except Exception as E:
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        XLOGDB.commit()

#
# doneWithFile
#
def doneWithFile(filename):
    """Move filename to DONESD."""
    me = 'doneWithFile(%s)' % repr(filename)
    _sl.info(me)
    moved = False   # Pessimistic.
    try:

        # Moving?
        if not DONESD:
            return

        # SRC, SNK.
        src = os.path.normpath(WPATH + '/' + filename)
        snk = os.path.normpath(WPATH + '/' + DONESD + '/' + filename)

        # No SRC, already SNK?
        if not os.path.isfile(src):
            _m.beeps(1)
            errmsg = 'src dne'
            _sl.warning(errmsg)
            return
        if os.path.isfile(snk):
            _m.beeps(1)
            errmsg = 'snk already in %s' % DONESD
            _sl.warning(errmsg)
            return


        # Do the move.  A failure is squawked and tolerated.
        try:
            shutil.move(src, snk)
            moved = True
        except Exception as E:
            moved = False
            _m.beeps(3)
            errmsg = 'moving %s to %s failed: %s' % (filename, DONESD, E)
            _sl.warning(errmsg)
            pass                    # POR.

        # Still SRC, no SNK?
        if os.path.isfile(src):
            moved = False
            _m.beeps(1)
            errmsg = 'src not moved'
            _sl.warning(errmsg)
            return
        if not os.path.isfile(snk):
            moved = False
            _m.beeps(1)
            errmsg = 'snk dne in %s' % DONESD
            _sl.warning(errmsg)
            return

    except Exception as E:
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        if moved:
            FFWDB.delete(filename)
#
# loadrecs2db
#
def loadrecs2db():
    """Load a batch into db."""
    global LOADRECS, NOLOAD, NDUPE, NNEW
    try:    z = str(len(LOADRECS))
    except: z = 'None'
    me = 'loadrecs2db(%s)' % (z)
    try:
        if (not LOADRECS) or NOLOAD:
            return
        assert XLOGDB, 'no XLOGDB'
        for lr in LOADRECS:
            sha1 = lr[6]        
            if len(sha1) != 40:
                lr = lr
                raise ValueError('funny SHA1: ' + repr(sha1))
            # Already?
            try:
                c = XLOGDB.cursor()
                c.execute('select count(*) from xlog where sha1=%s', (sha1,))
                if c.fetchone()[0]:
                    NDUPE += 1
                    continue
            finally:
                c.close()
            # Insert into [xlog].
            try:
                c = XLOGDB.cursor()
                sql = inssqlxlog  = 'insert into xlog (%s) values (%s)' % (DB_FNL_XLOG, DB_FIL_XLOG)
                c.execute(sql, lr)
                NNEW += 1
            finally:
                c.close()
        pass
    except Exception as E:
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        XLOGDB.commit()
        LOADRECS = []

#
# logrec2fields
#
def logrec2fields(logrec):
    """Extract fields (which must exist, except when logrec is a ';' comment) from logrec."""
    fv = rxts = txts = srcid = subid = el = sl = sha1 = kvs = None
    try:
        # Comment?
        if logrec[0] == ';':
            kvs = logrec;                       # (ab)Use kvs field.
            return
        # Format value?
        if logrec[1] == '\t' and logrec[0].isdigit():
            fv = int(logrec[0])
            if  fv == 1:
                nt = logrec.count('\t')
                if nt == 9:             # To handle a redundant sl in older preambles.
                    (rxts, txts, srcid, subid, z, el, sl, sha1, kvs) = logrec[2:].split('\t', 8)
                    raise ValueError('bad extra z (%s) vs sl (%s)' % (repr(z), repr(sl)))
                else:
                    (rxts, txts, srcid, subid, el, sl, sha1, kvs) = logrec[2:].split('\t', 7)
            else:
                raise ValueError('bad ffv: %d' % fv)
        else:
            fv, subid = 0, '____'                                           # !MAGIC! defaults.
            (rxts, txts, srcid, el, sl, sha1, kvs) = logrec.split('|', 6)
    finally:
        try:    sha1 = sha1.lower()     # Safety.
        except: sha1 = None
        return (fv, rxts, txts, srcid, subid, el, sl, sha1, kvs)

#
# logrec2loadrecs
#
# a: '1~1449937065.5425~1429603009.    ~nx01~____~0~a~661ee11b77bcb896160847d26f8f29eac778c6d0~{"_el": "0", "_id": "nx01", "_ip": "192.168.100.6", "_si": "____", "_sl": "a", "_ts": "1429603009.    ", "ae": "a", "body_bytes_sent": 0, "http_referer": null, "http_user_agent": "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36", "remote_addr": "142.179.5.254", "remote_user": null, "request": "GET / HTTP/1.1", "status": 200, "time_local": "[21/Apr/2015:00:56:49 -0700]", "time_utc": 1429603009}'
# e: '1~1449937092.0761~1430060751.    ~nx01~____~0~e~2d8d1654f1e0fc5d91b691917241272207c8475b~{"_el": "0", "_id": "nx01", "_ip": "192.168.100.6", "_si": "____", "_sl": "e", "_ts": "1430060751.    ", "ae": "e", "status": "[error]", "stuff": "2697#0:\\t*2062\\topen()\\t\\"/var/www/184.69.80.202/cgi-bin/php\\"\\tfailed\\t(2:\\tNo\\tsuch\\tfile\\tor\\tdirectory),\\tclient:\\t52.6.9.169,\\tserver:\\t184.69.80.202,\\trequest:\\t\\"GET /cgi-bin/php HTTP/1.1\\",\\thost:\\t\\"184.69.80.202\\"", "time_local": "2015/04/26 08:05:51", "time_utc": 1430060751}'
# h: '1~1449937097.9012~1449937097.9118~nx01~____~0~h~657bffdd300e888ce426dbe3b0555306b2601ebb~{"_el": "0", "_id": "nx01", "_ip": "192.168.100.6", "_si": "____", "_sl": "h", "_ts": "1449937097.9118", "ae": "h", "dt_loc": "2015-12-12 08:18:17.9118", "dt_utc": "2015-12-12 16:18:17.9118"}'
# x: '1~1449544070.0674~1449544070.0674~----~----~0~_~e49766fcacb04c1de185eb8433a2ea8e625fbe2c~{"_el": 0, "_id": "----", "_ip": "0.0.0.0", "_msg": "main begins @ 2015-12-07 19:07:50", "_si": "----", "_sl": "_", "_ts": "1449544070.0674"}'
#     | |               |               |    |    | | |                                         |
#     | |               |               |    |    | | |                                         kvs
#     | |               |               |    |    | | sha1 of kvs
#     | |               |               |    |    | error sublevel
#     | |               |               |    |    error level      
#     | |               |               |    client subid 
#     | |               |               client (source) id
#     | |               tx (clint) timestamp
#     | rx (server) timestamp
#     flat file format version   
#
#     1~rxts~txts~srcid~subid~el~sl~sha1~kvs       
#
def logrec2loadrecs(logrec):                               
    """Convert logrec and add to loadrecs."""
    global LOADRECS

    try:

        try:    logrec = logrec.rstrip()    # No \n.
        except: logrec = None
        if not logrec:
            return

        # Split logrec to fields.
        (fv, rxts, txts, srcid, subid, el, sl, sha1, kvs) = logrec2fields(logrec)
        if fv is None:
            if kvs:
                _sl.extra(kvs)      # Comment.
            return
        rxts2, txts2 = float(rxts), float(txts)

        # Heartbeat? 
        if el == HB_EL and sl == HB_SL:   
            addHeartbeat(logrec)
            return
        # Debug.
        else:
            if   logrec.find('"a"') > -1:
                logrec = logrec
            elif logrec.find('"e"') > -1:
                logrec = logrec
            else:
                logrec = logrec

        # Stash info for db loading (via batch commits).

        #
        # (
        # rxts   '1449993602.7583', 
        # txts   '1449993602.7554', 
        # srcid  'nx01', 
        # subid  '____', 
        # el     '0', 
        # sl     'h', 
        # sha1   '9b8328e4bbd014c6e50786b00d0672aa81bead47',
        # kvs    '{"_el": "0", "_id": "nx01", "_ip": "192.168.100.6", "_si": "____", "_sl": "h", "_ts": "1449993602.7554", "ae": "h", "dt_loc": "2015-12-13 00:00:02.7554", "dt_utc": "2015-12-13 08:00:02.7554"}
        # )
        #

        z = [_S(rxts2), _S(txts2), _S(srcid), _S(subid), _S(el), _S(sl), _S(sha1), _S(kvs)]  # !!! Matches xlog table.
        LOADRECS.append(z)

        # Commit batch?
        if len(LOADRECS) >= LOADCOMMITBATCHSIZE:
            loadrecs2db()

    except Exception as E:
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        pass

#
# Export a file, either history (whole file) or live (incremental).
#
def exportFile(historical, xfi):
    """Export a file (from info dict)."""
    fn = xfi['filename']
    me = 'exportFile(%s, %s)' % (str(historical), fn)
    _sl.info('%s  %s  %s' % (_dt.ut2iso(_dt.locut()), fn, 'h' if historical else ''))#$#
    nb2e = 0                            # Finally references.
    try:

        # Safety flush.
        loadrecs2db()  

        # How many bytes of file is to be exported?
        fskip = xfi['processed']
        fsize = xfi['size']
        nb2e = fsize - fskip
        if nb2e <= 0:
            return

        # 2/2: Optionally append stuff to logfile to 
        # simulate intraprocess mofification.
        # 151213-02.log -> 50,262 bytes -> 51,630 bytes.

        # Still exists?
        pfn = os.path.normpath(WPATH + '/' + fn)
        if not os.path.isfile(pfn):
            _m.beeps(1)
            errmsg = '%s: file dne' % me
            _sl.warning(errmsg)
            return                  

        # .gz files are always treated as historical, and 
        # the whole file is read. (No seek!)
        if pfn.endswith('.gz'):          
            with gzip.open(pfn, 'r', encoding=ENCODING, errors=ERRORS) as f:
                for x, logrec in enumerate(f):
                    if not (x % 1000):
                        _sw.iw('.')
                    logrec2loadrecs(logrec)
            return

        # Uncompressed files are read as text, with an initial
        # seek from the SOF. The file is read to its end, even
        # if this goes beyond the size given, which will 
        # happen if XLOG appends to this file while we're 
        # processing it. This will result in the appendage 
        # being reprocessed next time around, but this is 
        # harmless bcs logrec and heartbeat processing is 
        # able to handle (skip) duplicates.
        with open(pfn, 'r', encoding=ENCODING, errors=ERRORS) as f:
            if fskip > 0:
                _sl.info('skipping {:,d} bytes'.format(fskip))
                f.seek(fskip)
            for x, logrec in enumerate(f):
                if not (x % 1000):
                    _sw.iw('.')
                logrec2loadrecs(logrec)

    except Exception as E:
        nb2e *= -1                      # Prevent 'processed' update.
        errmsg = '%s: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        # End dots.
        _sw.nl()                   
        # Close src file.
        try:  f.close
        except:  pass
        # Update 'processed'?
        if nb2e > 0 and not TESTONLY:
            xfi['processed'] = fsize
            z = {'filename': xfi['filename'], 'processed': xfi['processed']}
            FFWDB.update(z)
        # Flush heartbeats and loadrecs.
        flushHeartbeats()
        loadrecs2db()

#
# ownHeartbeat
#
# x: '1~1449937097.9012~1449937097.9118~nx01~____~0~h~657bffdd300e888ce426dbe3b0555306b2601ebb~{"_el": "0", "_id": "nx01", "_ip": "192.168.100.6", "_si": "____", "_sl": "h", "_ts": "1449937097.9118", "ae": "h", "dt_loc": "2015-12-12 08:18:17.9118", "dt_utc": "2015-12-12 16:18:17.9118"}'
# h: '1~1450197462.2019~1450197462.2019~xlog~2db_~0~h~27ac26747d5d70f5e1caa84d9b3026a6a14cd417~{"_el": "0", "_id": "xlog", "_ip": null, "_si": "2db_", "_sl": "h", "_ts": "1450197462.2019", "dt_loc": "2015-12-15 08:37:42.2019", "dt_utc": "2015-12-15 16:37:42.2019"}'
#
def ownHeartbeat(uu=None):
    me = 'ownHeartbeat'
    logrec = None
    try:
        if uu is None:
                uu = _dt.utcut()
        ul = _dt.locut(uu)
        uuts = '%15.4f' % uu                                # 15.4, unblanked fraction.
        uuiosfs = _dt.ut2isofs(uu)
        uliosfs = _dt.ut2isofs(ul)
        rxts = txts = uuts
        kvs = {'_id': SRCID, '_si': SUBID, 
               '_el': HB_EL, '_sl': HB_SL, 
               '_ip': None, '_ts': uuts, 
               'dt_loc': uliosfs, 'dt_utc': uuiosfs}
        kvsa = json.dumps(kvs, ensure_ascii=True, sort_keys=True)
        kvsab = kvsa.encode(encoding=ENCODING, errors=ERRORS)
        h = hashlib.sha1()
        h.update(kvsab)
        sha1x = h.hexdigest()
        _ = '\t'
        logrec = '%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' %('1', _, rxts, _, txts, _, SRCID, _, SUBID, _, HB_EL, _, HB_SL, _, sha1x, _, kvsa)
    except Exception as E:
        errmsg = '%s: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        return logrec

#
# watcherThread
#
FWTRUNNING = False  # File Watcher Thread Running.
FWTSTOP = False     # To signal a shutdown.
FWTSTOPPED = False  # To acknowledge a shutdown.
def watcherThread():
    """A thread to watch WPATH for files to process."""
    global LOADRECS, FFWDB, FWTRUNNING, FWTSTOP, FWTSTOPPED

    LOADRECS = []

    me = 'watcher thread' 
    try:
        FWTRUNNING = True
        assert XLOGDB, 'no XLOGDB'

        # Connect to FlatFileWatchDataBase.
        FFWDB = ffwdb.FFWDB(FFWDBPFN)
        assert FFWDB, 'no FFWDB'

        uu = 0                                                  # Unix Utc.
        while not FWTSTOP:

            #
            flushHeartbeats()
           
            # Wait out INTERVAL.
            z = time.time()
            w = INTERVAL - (z - uu)
            if w > 0:
                _sw.wait(w)
            uu = _dt.utcut()
            ul = _dt.locut(uu)
            uuts = '%15.4f' % uu                                # 15.4, unblanked fraction.
            uuiosfs = _dt.ut2isofs(uu)
            uliosfs = _dt.ut2isofs(ul)

            # Heartbeat?
            if OWNHEARTBEAT:
                logrec = ownHeartbeat(uu)
                addHeartbeat(logrec)

            # Files?
            t0 = time.perf_counter();
            fis = getFIs(uu)
            t1 = time.perf_counter();
            if TIMINGS:
                _sl.warning('   getFIs: {:9,.1f} ms'.format((1000*(t1-t0))))
            if not fis:
                errmsg = 'no logfiles @ ' + uliosfs
                raise Exception(errmsg)

            # Update FFWDB. 
            # Freshen the "acquired" timestamp.
            # Delete entries for nonexistent files.
            t0 = time.perf_counter();
            filenames = [fi['filename'] for fi in fis]
            filenames.sort()
            # Compare real (nf) and db (nx) counts.
            nf = len(filenames)
            nx = FFWDB.count()
            if   nf < nx:
                nf, nx = nf, nx
            elif nf > nx:
                nf, nx = nf, nx
            else:
                nf, nx = nf, nx
            FFWDB.acquired(filenames, uu)
            for fi in fis:
                z = updateDB(fi)
            t1 = time.perf_counter();
            if TIMINGS:
                _sl.warning('updateDBs: {:9,.1f} ms'.format((1000*(t1-t0))))

            # Find the oldest, newest unfinished files in DB.
            t0 = time.perf_counter();
            o_dbfi, n_dbfi = FFWDB.oldestnewest('u')
            t1 = time.perf_counter();
            if TIMINGS:
                _sl.warning('   oldest: {:9,.1f} ms'.format((1000*(t1-t0))))

            # Something unfinished?
            if o_dbfi:  

                fn = o_dbfi['filename']
                historical = (fn != n_dbfi['filename'])

                # Export the file.
                exportFile(historical, o_dbfi)    # !CHANGE!

            # Move oldest finished file? 
            # It must not be the only, and therefore "live", file.
            if DONESD:
                o_dbfi, n_dbfi = FFWDB.oldestnewest('f')
                if o_dbfi and (n_dbfi['filename'] != o_dbfi['filename']):
                    t0 = time.perf_counter();
                    doneWithFile(o_dbfi['filename'])
                    t1 = time.perf_counter();
                    if TIMINGS:
                        _sl.warning('    moved: {:9,.1f} ms'.format((1000*(t1-t0))))

            if ONECHECK:
                FWTSTOP = True

    except KeyboardInterrupt as E:
        _m.beeps(1)
        # watcherThread:
        msg = '1: {}: KeyboardInterrupt: {}'.format(me, E)
        _sl.warning(msg)
        ###---DOSQUAWK(errmsg, beeps=1)
        pass###raise                # Let the thread exit.  Avoids "Exception in thread...".
    except Exception as E:
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise      
    finally:
        # Flush heartbeats and loadrecs.
        flushHeartbeats()
        loadrecs2db()
        if FWTSTOP:
            FWTSTOPPED = True
        FFWDB.disconnect()
        _sl.info('%s exits. STOPPED: %s' % (me, str(FWTSTOPPED)))
        FWTRUNNING = False

#
# getFI
#
def getFI(fn, ts):
    """Return a FileInfo dict for _fn."""
    me = 'getFI(%s)' % repr(fn)
    fi = None
    try:
        if not REFNPATTERN.match(fn):
            errmsg = 'invalid filename: %s' % fn
            raise Exception(errmsg)
        yymmdd = fn[:6]
        ymd = '20' + yymmdd
        hh = fn[7:9]
        pfn = os.path.normpath(WPATH + '/' + fn)
        try:
            st    = os.stat(pfn)
            size  = st.st_size
            mtime = st.st_mtime
        except Exception as E:
            errmsg = 'stat(%s) failed' % pfn
            raise Exception(errmsg)
        fi = {'filename': fn,
              'ymd': ymd,
              'hh': hh,
              'modified': mtime,
              'size': size,
              'acquired': ts}
    except Exception as E:
        fi = None               # Zap!
        errmsg = '%s: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        return fi

#
# getFIs
#
def getFIs(ts):
    """Return a list of FileInfo dicts of current files."""
    me = 'getFIS'
    fis = []
    try:
        for filename in sorted([fn for fn in os.listdir(WPATH) if REFNPATTERN.match(fn)]):
            fi = getFI(filename, ts)
            if not fi:
                continue
            fis.append(fi)
    except Exception as E:
        fis = None              # ??? Zap all?
        errmsg = '%s: %s @ %s' % (me, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise
    finally:
        return fis

#
# main: xlog2db
#
def xlog2db():
    global SRCID, SUBID, WPATH, DONESD, INTERVAL
    global FFWDBPFN, FWTSTOP, FWTSTOPPED, XLOGDB
    me, action = 'xlog2db', ''
    try:
        _sl.info(me + ' begins')#$#
        SRCID = _a.ARGS['--srcid']
        SUBID = _a.ARGS['--subid']
        WPATH = _a.ARGS['--wpath'].rstrip('/').rstrip('/')
        DONESD = _a.ARGS['--donesd']
        INTERVAL = float(_a.ARGS['--interval'])
        DBCFG = _a.ARGS['--xlogdb']         # DB connection configuration, as a string.
        DBCFG = eval(DBCFG)                 # ..., as a dict.

        _sl.info()
        _sl.info('    srcid: ' + SRCID)
        _sl.info('    subid: ' + SUBID)
        _sl.info('    wpath: ' + WPATH)
        _sl.info('  done sd: ' + DONESD)
        _sl.info(' interval: ' + str(INTERVAL))
        _sl.info('   db cfg: ' + repr(DBCFG))
        _sl.info()

        # FFW DB PFN.  DB creation must be done in watcherThread.
        FFWDBPFN = os.path.normpath(WPATH + '/xlog2db.s3')

        # Open sink db.
        XLOGDB = mc.connect(host=DBCFG['host'], user=DBCFG['user'], password=DBCFG['password'], db=DBCFG['database'], raise_on_warnings=True)

        # Start watcher() in a thread.

        watcher_thread = threading.Thread(target=watcherThread)
        watcher_thread.start()
        # Wait for startup.
        while not FWTRUNNING:           
            time.sleep(0.010)          

        # Wait for shutdown.
        while FWTRUNNING:
            time.sleep(1)

        # Ctrl-c to stop & exit.

    except KeyboardInterrupt as E:
        _m.beeps(1)
        msg = '2: {}: KeyboardInterrupt: {}'.format(me, E)
        _sl.warning(msg)
        ###---DOSQUAWK(errmsg, beeps=1)
        pass###raise
    except Exception as E:
        errmsg = '{}: E: {} @ {}'.format(ME, E, _m.tblineno())
        DOSQUAWK(errmsg)
        raise                  
    finally:
        if watcher_thread and FWTRUNNING:
            FWTSTOP = True
            watcher_thread.join(3 * INTERVAL)
            _sl.info('thread STOPPED: %s' % FWTSTOPPED)

if __name__ == '__main__':


    # Test 1.
    if False:

        1/1

        SRCID = 'xlog'
        SUBID = '2db_'
        DONESD = None
        INTERVAL = 6

        WPATH = 'c:/xlog/test'
        FFWDBPFN = os.path.normpath(WPATH + '/xlog2db.s3')
        FFWDB = ffwdb.FFWDB(FFWDBPFN)

        DBCFG = {'driver': 'MYSQLDIRECT', 'user': 'root', 'password': 'woofuswoofus', 'host': '192.168.100.5', 'database': 'XLOG'}
        XLOGDB = mc.connect(host=DBCFG['host'], user=DBCFG['user'], password=DBCFG['password'], db=DBCFG['database'], raise_on_warnings=True)

        logrec = ownHeartbeat()
        addHeartbeat(logrec)
        flushHeartbeats()

        testfn = '151213-00.log'    # 374,999 bytes.
        testfn = '151213-01.log'    # 174,586 bytes.
        testfn = '151213-02.log'    #  48,894 bytes.
        historical = False

        testfi = FFWDB.select(testfn)
        ###!!!testfi['processed'] = 0

        # 1/2: Optionally append stuff to logfile to 
        # simulate intraprocess mofification.
        # 151213-02.log -> 50,262 bytes.
        # Doing this here causes no export bcs testfi 
        # shows the file a fully processed.

        if False:
            # Update FFWDB and reget testfi.
            uu = _dt.utcut()
            fis = getFIs(uu)
            filenames = [fi['filename'] for fi in fis]
            filenames.sort()
            FFWDB.acquired(filenames, uu)
            for fi in fis:
                if fi['filename'] == testfn:
                    testfi = updateDB(fi)
        
        exportFile(historical, testfi)
 
        shutDown()

        1/1

    # Test 2.
    if False:
        WPATH = 'c:/xlog/test/'
        DONESD = 'XL2DB'
        FFWDBPFN = os.path.normpath(WPATH + '/xlog2db.s3')
        FFWDB = ffwdb.FFWDB(FFWDBPFN)
        fn = '151207-19.log'
        doneWithFile(fn)
        doneWithFile(fn)
        1/1

    # Production.
    if True:

        try:
            xlog2db()
        except KeyboardInterrupt as E:
            _m.beeps(1)
            msg = '3: {}: KeyboardInterrupt: {}'.format(ME, E)
            _sl.warning(msg)
            ###---DOSQUAWK(errmsg, beeps=1)
            pass###raise
        except Exception as E:
            errmsg = '{}: E: {}'.format(ME, E)
            DOSQUAWK(errmsg)
            raise
        finally:
            shutDown()
            1/1
