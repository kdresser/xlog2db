#> !P3!

#> 1v0

###
### xlog2db:
###
###         A File Watcher of xlog output flatfiles.
###         Incrementally processes those, adding them to a 
###         working db (xlog) table (xlog).
###         Processing of history files and an in-progress live 
###         log files is tracked in an sqlite db (xlog2db.s3).
###         In fw2, current reality is periodically stored in a 
###         sorted list of dicts called "snap" that, along with
###         the contents of logfiles in xlog2db.s3, drives the 
###         transfer to the destination db (xlog).
###         Unlike fw1, there is no separate single historical 
### 	    file processing.  Instead, all files in a watched 
###         folder are found and processed, oldest to newest. 
###         Completed files that are older than the newest live 
###         file are moved to folder fw2done and their metadata 
###         are deleted from the sqlite db. 
###         Xlog's logfile naming pattern ("yymmdd-hh.log") makes 
###         it easy to age-order files without refrencing file 
###         system timestamps.
###
###     Intended for use on Windows, but should work on Linux too.
###     No i-node tracking is done. Historical files will be 
###     reprocessed in their entirety if not finished. SHA1 hashes
###     in the db enables the skipping of duplicate logrecs during
###     reruns.
###
### 151101: FlatFile Version at front of records: 
###         FFV = '1': Version added, _si added, '\t' instead of '|'.
###

"""
Usage:
  me.py [--ini=<ini> --wpath=<wpath> --interval=<interval> --db=<db>]
  me.py (-h | --help)
  me.py --version

Options:
  -h --help              Show help.
  --version              Show version.
  --ini=<ini>            Overrides default ini pfn.
  --wpath=<wpath>        Path to be watched for "yymmdd-hh.log" pattern.
                         If a file, process as history.
  --interval=<interval>  Interval (seconds).
  --db=<db>              CFG of xlog database.
"""

import os, sys, stat
import time, datetime, calendar
import shutil
import collections
import pickle
import copy
import json
###---import configparser
import threading
import re
import gzip
###---import pytz

###---import docopt

gP2 = (sys.version_info[0] == 2)
gP3 = (sys.version_info[0] == 3)
assert gP3, 'requires Python 3'

gWIN = sys.platform.startswith('win')
gLIN = sys.platform.startswith('lin')

####################################################################################################

# Setup.

WPATH = '???'                       # Watched path.
INTERVAL = 6                        # Seconds.
ENCODING = 'utf-8'                 
ERRORS = 'strict'

import l_args as _a
ME = _a.get_args(__doc__, '0.1')

WPATH = _a.ARGS['--wpath']
INTERVAL = _a.ARGS['--interval']
DBCFG = _a.ARGS['--db']             # DB connection configuration, as a string.
DBCFG = eval(DBCFG)                 # ..., as a dict.
DB = None                           # DB connection.
INTERVAL = float(INTERVAL)

SNAPTS = 0                          # Timestamp of last snapshot and processing.
SNAPFILES = []                      # At each snapshot, a list of dicts, one for
                                    # each logifle still in the watched folder.
DONESUBDIR = 'XL2DB'                # XLog flatfiles loaded to db are moved from
                                    # the watched folder to this subdirectory.

import l_screen_writer
_sw = l_screen_writer.ScreenWriter()
import l_simple_logger
_sl = l_simple_logger.SimpleLogger(screen_writer=_sw)

SQUAWKED = False

import l_dt as _dt
import l_misc as _m

import mysql.connector as mc

DEBUG = True                        # Dev/test.

####################################################################################################

# >>> Table [xlog].  

DBFNSXLOG =     ('id', 'rxts', 'txts', 'srcid', 'subid', 'el', 'sl', 'sha1', 'kvs') 
DBFNLXLOG = DBFILXLOG = ''  # Field name list, %s insertion list.
for fn in DBFNSXLOG:
    if fn == 'id':
        continue
    if DBFNLXLOG:
        DBFNLXLOG += ', '
        DBFILXLOG += ', '
    DBFNLXLOG += fn
    DBFILXLOG += '%s'

LOADCOMMITBATCHSIZE = 1000

# >>> N-action and 1-action committing connections.

DB = mc.connect(host=DBCFG['host'], user=DBCFG['user'], password=DBCFG['password'], db=DBCFG['database'], raise_on_warnings=True)

SCREENBATCHSIZE = 100

####################################################################################################

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

# A DB of logfiles' info.

# ('filename', 'ud', 'hh', 'created', 'updated', 'size', 'checked', 'processed', 'historical')

# Set up an sqlite3 db for file tracking.
# Note: sqlite3 db must be opened in the using thread.
FWDBPFN = os.path.normpath(WPATH + '/xlog2db.s3')
FW = None
import ffwdb as db
connectFW    = db.connectDB
disconnectFW = db.disconnectDB
countFW      = db.countDB
selectFW     = db.selectDB
filenamesFW  = db.filenamesDB
insertFW     = db.insertDB
updateFW     = db.updateDB
deleteFW     = db.deleteDB

####################################################################################################

# Filename pattern: yymmdd-hh.log

FNPATTERN = r'\d{6}-\d{2}\.log'    # or '\d{6}-\d{2}\.[lL][oO][gG]' or some other case insensitivity
REFNPATTERN = re.compile(FNPATTERN)

####################################################################################################

def closeOutput():
    pass

STOP = False        # To signal a shutdown.
STOPPED = False     # To acknowledge a shutdown.
def watcherThread():
    global FW, SNAPFILES, SNAPTS, STOPPED, SQUAWKED

    inssqlxlog  = 'insert into xlog (%s) values (%s)' % (DBFNLXLOG, DBFILXLOG)
    liidsqlxlog = 'select last_insert_id()'                                     # !MAGIC! (MySQL/MariaDB)

    ###############################################################

    # Batched logrecs to reduce the number of commits.
    def loadBatchedLogRecs(logrecs):
        1/1
        if not logrecs:
            return
        try:

            for vs in logrecs:

                txts = vs[2]        
                sha1 = vs[6]        
                
                if len(sha1) != 40:
                    vs = vs
                    raise ValueError('funny SHA1: ' + repr(sha1))

                # Already?
                c = DB.cursor()
                c.execute('select count(*) from xlog where sha1=%s', (sha1,))
                if c.fetchone()[0]:
                    continue

                # Insert into [xlog].
                c = DB.cursor()
                c.execute(inssqlxlog, vs)
                liidmain = c.lastrowid
            pass

        except Exception as E:
            _m.beeps(3)
            errmsg = '%s: processLogRecs: E: %s @ %s' % (me, E, _m.tblineno())
            _sl.error(errmsg)
            SQUAWKED = True
            raise
        finally:
            # Empty the logrecs list.
            logrecs.clear()
            # Commit the batch.
            DB.commit()

    def prepareOneLogRec(logrec, logrecs):
    
        # Split logrec to fields.
        try:
            if logrec[1] == '\t' and logrec[0].isdigit():
                ffv = int(logrec[0])
                if  ffv == 1:
                    ###!!!
                    nt = logrec.count('\t')
                    if nt == 9:             # To handle a redundant sl in the preamble.
                        (rxts, txts, srcid, subid, z, el, sl, sha1, kvs) = logrec[2:].split('\t', 8)
                        assert z == sl
                    else:
                        (rxts, txts, srcid, subid, el, sl, sha1, kvs) = logrec[2:].split('\t', 7)
                    ###!!!
                    ###(rxts, txts, srcid, subid, el, sl, sha1, kvs) = logrec[2:].split('\t', 7)
                else:
                    raise ValueError('bad ffv: %d' % ffv)
            else:
                ffv, subid = 0, '____'
                (rxts, txts, srcid, el, sl, sha1, kvs) = logrec.split('|', 6)
        except Exception as E:
            _m.beeps(3)
            errmsg = '%s: logrec.split: %s @ %s' % (me, E, _m.tblineno())
            _sl.error(errmsg)
            SQUAWKED = True
            raise
        rxts, txts = float(rxts), float(txts)

        # Stash info for db loading.
        ###---z = (_S(None), _S(rxts), _S(txts), _S(srcid), _S(subid), _S(el), _S(sl), _S(sha1), _S(kvs))
        ###---#       ^ ??? WTF?
        z = (_S(rxts), _S(txts), _S(srcid), _S(subid), _S(el), _S(sl), _S(sha1), _S(kvs))
        logrecs.append(z)

        # Load and commit in batches.
        if len(logrecs) >= LOADCOMMITBATCHSIZE:
            loadBatchedLogRecs(logrecs)

        pass

    def doneWithFile(fn):
        src = os.path.normpath(WPATH + '/' + fn)
        snk = os.path.normpath(WPATH + '/' + DONESUBDIR + '/' + fn)
        try:
            shutil.move(src, snk)
        except Exception as E:
            _m.beeps(3)
            errmsg = '%s moves %s to %s failed: %s' % (me, fn, DONESUBDIR, E)
            _sl.error(errmsg)
            SQUAWKED = True
            raise
        deleteFW(FW, fn)
        _sl.info('moved %s to %s' % (fn, DONESUBDIR))

    ###############################################################

    me = 'watcher thread'
    try:
        # Connect to fw2 database.
        FW = connectFW(FWDBPFN)

        while not STOP:

            # Wait out INTERVAL.
            cts = time.time()
            w = INTERVAL - (cts - SNAPTS)
            if w > 0:
                _sw.wait(w)
            SNAPTS = time.time()

            # ('filename', 'ud', 'hh', 'created', 'updated', 'size', 'checked', 'processed', 'historical')

            # Snapshot the current log files and add to or update the db where necessary.
            #$#ml.info()#$#
            SNAPFILES = []
            for filename in sorted([fn for fn in os.listdir(WPATH) if REFNPATTERN.match(fn)]):
                yymmdd = filename[:6]
                ud = _dt.yyyymmdd2ud('20' + yymmdd)
                hh = int(filename[7:9])
                yymmdd, ud, hh = yymmdd, ud, hh
                pfn = os.path.normpath(WPATH + '/' + filename)
                st    = os.stat(pfn)
                size  = st.st_size
                mtime = st.st_mtime
                ctime = st.st_ctime     # Works on windows, but not on Unix.  ??? Linux?
                # A subset of DB fields (just the ones that are relevant to a snapshot).
                snap = {'filename': filename, 
                        'ud': ud,
                        'hh': hh,
                        'created': ctime, 
                        'updated': mtime,
                        'size': size,
                        'checked': cts}
                #       'processed'
                #       'historical'
                SNAPFILES.append(snap)

            if not SNAPFILES:
                continue

            # Note the newest logfile.
            newestsnap = SNAPFILES[-1]
            newestfn = newestsnap['filename']

            # Process logfiles, oldest to newest.
            for snap in SNAPFILES:
                if STOP:
                    break
                snapfn = snap['filename']
                # Get, and possibly update, what DB knows.
                fwdbid = selectFW(FW, snapfn)
                if not fwdbid:
                    # Insert.
                    z = copy.copy(snap)
                    z['processed'] = 0
                    z['historical'] = 1 if snapfn < newestfn else 0
                    fwdbid = insertFW(FW, z)
                    z = None
                    assert fwdbid, 'DB insert failed for %s' % snapfn
                else:
                    # Update?
                    if  fwdbid['updated'] != snap['updated'] or fwdbid['size'] != snap['size'] or snapfn < newestfn:
                        z = {}
                        z['filename'] = snapfn
                        z['updated'] = snap['updated']
                        z['size'] = snap['size']
                        z['historical'] = 1 if snapfn < newestfn else 0
                        fwdbid = updateFW(FW, z)
                        z = None
                
                # Use dbid to drive file processing.

                # Historical files that haven't started processing will be 
                # read (line by line), starting from 0.

                hst = fwdbid['historical']
                snapfn = snapfn
                hst = hst
                if hst and fwdbid['processed'] == 0:
                    try:
                        snapfn = snapfn
                        pfn = os.path.normpath(WPATH + '/' + snapfn)
                        n, done = 0, False
                        logrecs = []                
                        with open(pfn, 'r', encoding=ENCODING, errors=ERRORS) as f:
                            while True:
                                logrec = f.readline()
                                if not logrec:
                                    done = True
                                    break
                                n += 1
                                prepareOneLogRec(logrec, logrecs)
                                if not (n % SCREENBATCHSIZE):
                                    _sw.iw(':')
                                if STOP:
                                    break
                                pass
                            pass
                        pass
                        if done:
                            fwdbid['processed'] = fwdbid['size']
                            fwdbid = updateFW(FW, fwdbid)
                            doneWithFile(snapfn)
                        continue                        # Done with this snap.
                    finally:
                        # Load any residue.
                        loadBatchedLogRecs(logrecs)

                # Incrementally process the current live file.
                       
                1/1
                snapfn = snapfn
                nb2p = fwdbid['size'] - fwdbid['processed']     # Number of bytes to process.
                if nb2p > 0:
                    try:
                        pfn = os.path.normpath(WPATH + '/' + snapfn)
                        with open(pfn, 'rb') as f:
                            f.seek(fwdbid['processed'])
                            # Binary read from 'processed' to new 'size'.
                            b2p = f.read(nb2p)
                            # Split and process all '\n' delimited logrecs as one commit batch.
                            n = 0
                            logrecs = []
                            for brec in b2p.split(b'\n'):
                                if not brec:
                                    continue
                                prepareOneLogRec(brec.decode(encoding=ENCODING, errors=ERRORS), logrecs)
                                n += 1
                                _sw.iw('.')#$#
                                if STOP:
                                    break
                                pass
                            else:
                                # All new data has been split and processed; force 'processed' to 'size'.
                                fwdbid['processed'] = fwdbid['size']
                                fwdbid = updateFW(FW, fwdbid) 
                            pass
                        if STOP:
                            _sl.info('%s signalled to stop' % me)
                            STOPPED = True
                            return
                        pass
                    finally:
                        # Load any residue.
                        loadBatchedLogRecs(logrecs)
                    pass
                # Done with?
                if hst and fwdbid['processed'] == fwdbid['size']:
                    doneWithFile(snapfn)

            pass
        pass

    except KeyboardInterrupt as E:
        _m.beeps(1)
        msg = '{}: KeyboardInterrupt: {}'.format(me, E)
        _sl.warning(msg)
        SQUAWKED = True
        raise
    except Exception as E:
        _m.beeps(3)
        errmsg = '%s: E: %s @ %s' % (me, E, _m.tblineno())
        _sl.error(errmsg)
        SQUAWKED = True
        raise
    finally:
        disconnectFW(FW)
        _sl.info('%s exits' % me)

def xlog2db():
    global PATH, INTERVAL, ODB, STOP
    global SQUAWKED
    me, action = 'xlog2db', ''
    try:
        _sl.info(me + ' begins')#$#
        _sl.info()
        _sl.info('    wpath: ' + WPATH)
        _sl.info(' interval: ' + str(INTERVAL))
        _sl.info('    dbcfg: ' + repr(DBCFG))
        _sl.info()

        # Start watcher() in a thread.

        watcher_thread = threading.Thread(target=watcherThread)
        watcher_thread.start()

        while True:
            time.sleep(1)

        # Ctrl-c to stop & exit.

    except KeyboardInterrupt as E:
        _m.beeps(1)
        msg = '{}: KeyboardInterrupt: {}'.format(ME, E)
        _sl.warning(msg)
        SQUAWKED = True
        raise
    except Exception as E:
        if not SQUAWKED:
            _m.beeps(3)
            errmsg = '{}: E: {} @ {}'.format(ME, E, _m.tblineno())
            _sl.error(errmsg)
            SQUAWKED = True         # Squelch the traceback.
            raise                   # May be a bad idea.
                                    
    finally:
        if watcher_thread:
            STOP = True
            watcher_thread.join(3 * INTERVAL)
            _sl.info('full stop: %s' % STOPPED)
        # Other shutdown stuff...

if __name__ == '__main__':

    try:
        xlog2db()
    except KeyboardInterrupt as E:
        msg = '{}: KeyboardInterrupt: {}'.format(ME, E)
        _sl.warning(msg)
    except Exception as E:
        if not SQUAWKED:
            errmsg = '{}: E: {}'.format(ME, E)
            _sl.error(errmsg)
        errmsg = 'aborted'
        _sl.error(errmsg)
        pass
    finally:
        closeOutput()
        1/1

### ST_DB_CFG = {'user': 'root', 'password': 'woofuswoofus', 'host': '192.168.100.5', 'database': 'XLOG'}

###
### --ini=xlog2db.ini  --path=???h:/xlogs???
###
