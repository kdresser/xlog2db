
# *** XLOG2DB version ***

# DB for FlatFile Watching: XLOG output flatfiles.
# Logfiles are id'd by a YYMMDD-HH.log filename.
# YYMMDD-HH is, inconsequentially, a local date-time.
# Logfiles are loaded into XLOG:xlog table and moved to a 
# XL2DB subdirectory.
# Note: There's a similar, but different, same-named module for 
#       nl2xlog.

import sqlite3

FNS = ('filename', 'ymd', 'hh', 'modified', 'size', 'acquired', 'processed')


class FFWDB():

    def __init__(self, ffwdbpfn):
        self.ffwdbpfn = ffwdbpfn
        self.db = sqlite3.connect(self.ffwdbpfn)
        self.db.execute("""
            create table if not exists logfiles (
                filename    text,
                ymd         text,
                hh          text,
                modified    real,
                size	    integer,
                acquired    real,
                processed	integer)
        """)
    
    def disconnect(self):
        try:  self.db.close()
        except:  pass
        
    def count(self, filename=None):
        try:
            csr = self.db.cursor()
            if filename:
                csr.execute('select count(*) from logfiles where filename=?', (filename, ))
            else:
                csr.execute('select count(*) from logfiles')
            try:  return csr.fetchone()[0]
            except:  return None
        finally:
            self.db.commit()
        
    def select(self, filename):
        rd = None
        try:
            self.db.row_factory = sqlite3.Row
            csr = self.db.cursor()
            csr.execute('select * from logfiles where filename=?', (filename, ))
            try:  
                rd = csr.fetchone()
                if not rd:
                    return
                z = {}
                for k in rd.keys():
                    z[k] = rd[k]
                rd = z
            except Exception as E:
                rd = None
                errmsg = 'FFWDB.select: %s @ %s' % (E, mi.tblineno())
                raise Exception(errmsg)
        finally:
            self.db.commit()
            return rd
        
    '''???
    def filenames(self):
        try:
            csr = self.db.cursor()
            csr.execute('select filename from logfiles order by modified')
            try:  return [z[0] for z in csr.fetchall()]
            except:  return []
        finally:
            self.db.commit()
    ???'''

    def insert(self, fi):
        try:
            filename = fi['filename']
            if self.count(filename):
                raise ValueError('FFWDB.insert: %d already in db' % (filename))
            ks, qs, vs = [], [], []
            for k, v in fi.items():
                ks.append(k)
                qs.append('?')
                vs.append(v)
            sql = 'insert into logfiles (%s) values (%s)' % (', '.join(ks), ', '.join(qs))
            csr = self.db.cursor()
            csr.execute(sql, vs)
        finally:
            self.db.commit()
            return self.select(filename)
        
    def update(self, fi):
        try:
            filename = fi['filename']
            if not self.count(filename):
                raise ValueError('FFWDB.update: %d not in db' % (filename))
            kvs, vs = '', []
            for k, v in fi.items():
                if k == 'filename':
                    continue
                if kvs:
                    kvs += ', '
                kvs += (k + '=?')
                vs.append(v)
            vs.append(filename)
            sql = 'update logfiles set %s where filename=?' % kvs
            csr = self.db.cursor()
            csr.execute(sql, vs)
        finally:
            self.db.commit()
            return self.select(filename)
        
    def delete(self, filename):
        try:
            csr = self.db.cursor()
            csr.execute('delete from logfiles where filename=?', (filename, ))
        finally:
            self.db.commit()
        
    def oldestnewest(self, afu):
        """Return oldest and newest fi's. afu: a)ll, f)inished, u)nfinished."""
        #
        # Note: Initially used "modified" to order, but in 
        #       testing several "filenames" ended up with
        #       the same "modified" (due to file copying).
        #       Since "filenames" are meant to be well behaved,
        #       ordering has been switched to that.
        #

        def foo(self, afu, on):
            me = 'ffwdb.foo(%s, %s)' % (repr(afu), repr(on))
            rd = None
            try:
                if   on == 'o':  z = 'asc'
                elif on == 'n':  z = 'desc'
                else:            raise Exception('bad on')
                self.db.row_factory = sqlite3.Row
                c = self.db.cursor()
                if   afu == 'a':  sql = 'select * from logfiles                           order by filename %s limit 1' % z
                elif afu == 'f':  sql = 'select * from logfiles where (processed >= size) order by filename %s limit 1' % z
                elif afu == 'u':  sql = 'select * from logfiles where (processed  < size) order by filename %s limit 1' % z
                else:             raise Exception('bad kind')
                c.execute(sql)
                rd = c.fetchone()
                if not rd:
                    return
                z = {}
                for k in rd.keys():
                    z[k] = rd[k]
                rd = z
            except Exception as E:
                rd = None
                errmsg = '%s: %s @ %s' % (me, E, mi.tblineno())
                raise Exception(errmsg)
            finally:
                try:    c.close()
                except: pass
                self.db.commit()
                return rd

        o_rd = n_rd = None
        o_rd = foo(self, afu, 'o')
        n_rd = foo(self, afu, 'n')
        return (o_rd, n_rd)

    def acquired(self, filenames, ts):
        # Delete entries for nonexistent files.
        # Update remaining files' "acquired" timestamp.
        if not (filenames and ts):
            return
        try:
            csr = self.db.cursor()
            if len(filenames) == 1:
                sql1 = 'delete from logfiles where filename!="%s"' % (filenames[0])
                sql2 = 'update logfiles set acquired=?'
            else:
                z = ['"%s"' % fn for fn in filenames]
                z = '(' + ', '.join(z) + ')'
                sql1 = 'delete from logfiles where filename not in %s' % z
                sql2 = 'update logfiles set acquired=?'
            csr.execute(sql1)
            csr.execute(sql2, (ts, ))
        finally:
            self.db.commit()
