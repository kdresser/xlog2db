
# DB for FlatFile Watching.
# Intended for watching logfiles identified by a 
# unix day (ud) and an hour of the day (hh).

import sqlite3

FNS = ('filename', 'ud', 'hh', 'created', 'updated', 'size', 'checked', 'processed', 'historical')

def connectDB(_dbpfn):
    1/1
    db = sqlite3.connect(_dbpfn)
    db.execute("""
        create table if not exists flatfiles (
            filename    text,
            ud  	    integer,
            hh  	    integer,
            created	    real,
            updated	    real,
            size	    integer,
            checked	    real,
            processed	integer,
            historical	integer)
    """)
    return db
    
def disconnectDB(_db):
    1/1
    try:  _db.close()
    except:  pass
    return None
        
def countDB(_db, _filename=None):
    1/1
    try:
        csr = _db.cursor()
        if _filename:
            csr.execute('select count(*) from flatfiles where filename=?', (_filename, ))
        else:
            csr.execute('select count(*) from flatfiles')
        try:  return csr.fetchone()[0]
        except:  return None
    finally:
        _db.commit()
        1/1
        
def selectDB(_db, _filename):
    1/1
    try:
        _db.row_factory = sqlite3.Row
        csr = _db.cursor()
        csr.execute('select * from flatfiles where filename=?', (_filename, ))
        try:  
            rd = csr.fetchone()
            if not rd:
                return None
            z = {}
            for k in rd.keys():
                z[k] = rd[k]
            return z
        except Exception as E:
            mi.beeps(3)
            errmsg = 'selectDB: %s @ %s' % (E, mi.tblineno())
            ml.error(errmsg)
            SQUAWKED = True
            return None
    finally:
        _db.commit()
        1/1
        
def filenamesDB(_db):
    1/1
    try:
        csr = _db.cursor()
        csr.execute('select filename from flatfiles order by filename asc')
        try:  return [z[0] for z in csr.fetchall()]
        except:  return []
    finally:
        _db.commit()
        1/1

def insertDB(_db, _kvs):
    1/1
    try:
        fn = _kvs['filename']
        if countDB(_db, fn):
            raise ValueError('insertDB: %s already in db' % (fn))
        ks, qs, vs = [], [], []
        for k, v in _kvs.items():
            ks.append(k)
            qs.append('?')
            vs.append(v)
        sql = 'insert into flatfiles (%s) values (%s)' % (', '.join(ks), ', '.join(qs))
        csr = _db.cursor()
        csr.execute(sql, vs)
    finally:
        _db.commit()
        return selectDB(_db, fn)
        1/1
        
def updateDB(_db, _kvs):
    1/1
    try:
        fn = _kvs['filename']
        if not countDB(_db, fn):
            raise ValueError('updateDB: %s not in db' % (fn))
        kvs, vs = '', []
        for k, v in _kvs.items():
            if k == 'filename':
                continue
            if kvs:
                kvs += ', '
            kvs += (k + '=?')
            vs.append(v)
        vs.append(fn)
        sql = 'update flatfiles set %s where filename=?' % kvs
        csr = _db.cursor()
        csr.execute(sql, vs)
    finally:
        _db.commit()
        return selectDB(_db, fn)
        1/1
        
def deleteDB(_db, _filename):
    1/1
    try:
        csr = _db.cursor()
        csr.execute('delete from flatfiles where filename=?', (_filename, ))
    finally:
        _db.commit()
        1/1
