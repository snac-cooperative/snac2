#!/usr/bin/python

import decimal, re, datetime, hashlib, os.path, dateutil.parser, pickle
import urllib, urllib2, httplib
from sqlalchemy import Table, Column, Integer, DateTime, Date, ForeignKey, create_engine, UniqueConstraint#,MetaData 
from sqlalchemy import orm, types, func
from sqlalchemy.exc import InvalidRequestError, IntegrityError
from sqlalchemy.schema import DDL, FetchedValue, DefaultClause
from sqlalchemy.sql.expression import and_, literal_column, or_, text
from sqlalchemy.sql.functions import sum as sum_, count as count_
from sqlalchemy.ext.associationproxy import association_proxy

#from sqlalchemy.util import classproperty
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

import logging
import snac2.config.app as app_config
import snac2.config.meta as meta
import snac2.noid
import snac2.viaf
import snac2.utils as utils

import lxml.etree as etree

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def init_model(db_uri):
    """Call this before using any of the tables or classes in the model."""
    engine = create_engine(db_uri, echo=app_config.DEBUG_ECHO, pool_size = 100, pool_recycle=600)
    meta.session_maker = orm.sessionmaker(autoflush=True, autocommit=False, bind=engine)
    meta.engine = engine
    meta.Session = orm.scoped_session(meta.session_maker)
    meta.Base.metadata.bind = engine#create_engine(db_uri, pool_size = 100, pool_recycle=3600)

def create_model():
    """initialize the database schema; do not call outside init script"""
    return meta.Base.metadata.create_all()

def rollback(session=None):
    if not session:
        session = meta.Session
    session.rollback()

def commit(session=None):
    if not session:
        session = meta.Session
    try:
        session.commit()
    except Exception, e:
        session.rollback()
        raise e
    
def flush(session=None):
    if not session:
        session = meta.Session
    try:
        session.flush()
    except Exception, e:
        session.rollback()
        raise e


class Entity(object):
    
    @classmethod
    def get_by_id(cls, id, options=None, session=None):
        if not session:
            session=meta.Session
        try:
            q = session.query(cls)
            if options:
                q = q.options(*options)
            entity = q.filter(cls.id == id).one()
            return entity
        except NoResultFound:
            return None
    
    @classmethod
    def get_all_by_ids(cls, ids, options=None, session=None):
        if not session:
            session = meta.Session
        if not ids:
            return []
        q = session.query(cls)
        if options:
            q = q.options(*options)
        q = q.filter(cls.id.in_(ids))
        ids = [str(int(id)) for id in ids]
        ids = ", ".join(ids)
        # TODO: this is probably highly unsafe if IDs cannot be trusted; use this only in conjunction with internal search server or known good ints
        #q = q.order_by(cls.id)
        q = q.order_by("FIELD (%s.%s, %s)" % (cls.__tablename__, cls.id.__clause_element__().name, ids))
        return q.all()

    @classmethod
    def get_all_by_ids_starting_with(cls, id, options=None, session=None, iterate=False, limit=None, offset=None, only_ids=True):
        if not session:
            session = meta.Session
        q = session.query(cls)
        if options:
            q = q.options(*options)
        q = q.filter(cls.id >= id)
        q = q.order_by(cls.id.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        if iterate:
            return q
        return q.all()
    
    @property
    def entity_key(self):
        return self.id
        
    @classmethod
    def get_all(cls, options=None, session=None, iterate=False):
        if not session:
            session = meta.Session
        q = session.query(cls)
        if options:
            q = q.options(*options)
        if iterate:
            return q
        return q.all()

#     def to_dict(self, extra_vals=None, mode="basic", base_collection_name="__basic_attrs__", exclude_keys=None):
#         '''Converts Entity object graph into nested dictionaries for JSON processing using object introspection
#             mode: selects between full/min/basic attrs.  Defaults to basic_attrs if the mode does not exist
#             exclude_keys: keys that match this string are excluded from all objects in the object graph
#         '''
#         #d = {'entity_type':self.__class__.__name__}
#         collection_name = base_collection_name
#         if mode == "full":
#             collection_name = "__full_attrs__"
#         elif mode == "min":
#             collection_name = "__min_attrs__"
#         d = {}
#         exclude_keys = {} if not exclude_keys else exclude_keys
#         collection = None
#         if collection_name in self.__class__.__dict__:
#             collection = self.__class__.__dict__.get(collection_name)
#         elif base_collection_name in self.__class__.__dict__:
#             collection = self.__class__.__dict__.get(base_collection_name)
#         else:
#             collection = self.__dict__
#             
#         for k in collection:
#             if not (k.startswith(u"_") or (k in exclude_keys)):
#                 v = self.__getattribute__(k)
#                 if isinstance(v, Entity):
#                     v = v.to_dict(mode=mode, exclude_keys=exclude_keys)
#                 if isinstance(v, list):
#                     v = [i.to_dict() for i in v]
#                 if isinstance(v, dict):
#                     for v_key in v:
#                         v[v_key] = v[v_key].to_dict() if isinstance(v[v_key], Entity) else v[v_key]
#                 d[k] = v
#         if extra_vals:
#             d.update(extra_vals)
#         return d
#     
#     def to_json(self, extra_vals=None, mode="basic", exclude_keys=None):
#         return utils.json_serialize(self.to_dict(extra_vals=extra_vals, mode=mode, exclude_keys=exclude_keys))
    
    def save(self, session=None):
        if not session:
            session = meta.Session
        session.add(self)


    def refresh(self, session=None):        
        if not session:
            session = meta.Session
        session.refresh(self)
    
    def delete(self, session=None):
        if not session:
            session = meta.Session
        meta.Session.delete(self)
    
        
RECORD_TYPE_PERSON = u'person'
RECORD_TYPE_CORPORATE=u'corporateBody'
RECORD_TYPE_FAMILY=u'family'

RECORD_DATE_TYPE_ACTIVE = "http://socialarchive.iath.virginia.edu/control/term#Active"
RECORD_DATE_TYPE_BIRTH = "http://socialarchive.iath.virginia.edu/control/term#Birth"
RECORD_DATE_TYPE_DEATH = "http://socialarchive.iath.virginia.edu/control/term#Death"

class OriginalRecord(meta.Base, Entity):
    __tablename__ = 'original_records'
    id = Column(types.BigInteger, primary_key=True)
    name = Column(types.Unicode(512), nullable=False, server_default=u'')
    name_norm = Column(types.Unicode(512), nullable=False, index=True, server_default=u'')
    '''CREATE INDEX name_trgm_idx ON original_records USING gist (name_norm gist_trgm_ops);'''
    source_id = Column(types.Unicode(255), nullable=True, index=True, unique=True)
    collection_id = Column(types.Unicode(64), nullable=True, index=True)
    path = Column(types.Unicode(255), nullable=True, index=True)
    r_type = Column(types.Unicode(64), nullable=False, index=True)
    from_date = Column(Date(), nullable=True)
    from_date_type = Column(types.Unicode(64), nullable=True, index=True)
    to_date = Column(Date(), nullable=True)
    to_date_type = Column(types.Unicode(64), nullable=True, index=True)
    processed = Column(types.Boolean, nullable=False, index=True, server_default="false")
    record_data = orm.deferred(Column(types.UnicodeText, nullable=True))
    created_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, index=True)
    record_group_id = Column(types.BigInteger, ForeignKey('record_groups.id', onupdate="CASCADE", ondelete="SET NULL"), nullable=True, index=True )
    record_group_conf = Column(types.Numeric, server_default="0")
    
    __mapper_args__ = {
        'polymorphic_on':r_type
    }
    
    def __repr__(self):
        return "<OriginalRecord %d %s>" % (self.id, self.name.encode('utf-8'))
    
    @classmethod
    def load_from_eac(cls, eac_text):
        import xml.dom.minidom
        doc = xml.dom.minidom.parseString(eac_text)
        
        #cpf description
        cpfDescription = doc.getElementsByTagName("cpfDescription")
        identity = cpfDescription[0].getElementsByTagName("identity")
        
        #type
        entityType = identity[0].getElementsByTagName("entityType")
        entityType = entityType[0].childNodes[0].nodeValue
        record_classes = {RECORD_TYPE_PERSON:PersonOriginalRecord, RECORD_TYPE_CORPORATE:CorporateOriginalRecord, RECORD_TYPE_FAMILY:FamilyOriginalRecord}
        record_class = record_classes.get(entityType)
        if not record_class:
            return None
        
        extractNodeValue = lambda xmlNodeList: (xmlNodeList[0].childNodes[0].nodeValue)  if len(xmlNodeList) > 0 and len(xmlNodeList[0].childNodes) > 0 else None
        
        #ID
        entityId = doc.getElementsByTagName("recordId")
        entityId = entityId[0].childNodes[0].nodeValue
        
        #name
        names = []
        nameEntries = identity[0].getElementsByTagName("nameEntry")
        for nameEntry in nameEntries:
            namePart = extractNodeValue(nameEntry.getElementsByTagName("part"))
            if namePart != None:
                names.append(namePart)
            if nameEntry.attributes.get("localType") and  nameEntry.attributes["localType"].value == "http://socialarchive.iath.virginia.edu/control/term#Matchtarget":
                names = [namePart] # if there is a match target, just use that one and stop scanning
                break
            
        #dates
        existDates = doc.getElementsByTagName("existDates")
        fromDate = None
        toDate = None
        from_date_type = None
        to_date_type = None
        for existDate in existDates:
            fromDateElement = existDate.getElementsByTagName("fromDate")
            toDateElement = existDate.getElementsByTagName("toDate")           
            if fromDateElement and len(fromDateElement[0].attributes):
                fromDate = fromDateElement[0].attributes["standardDate"].value
                fromDate = dateutil.parser.parse(fromDate, default=datetime.date(1, 1, 1)) # default if out of bound
                from_date_type = fromDateElement[0].attributes["localType"].value
            if toDateElement and len(toDateElement[0].attributes):
                toDate = toDateElement[0].attributes["standardDate"].value
                toDate = dateutil.parser.parse(toDate, default=datetime.date(1, 12, 31)) # default if out of bound
                to_date_type = toDateElement[0].attributes["localType"].value
            
        return record_class(name=names[0], name_norm=utils.normalize_with_space(names[0]), source_id=entityId, from_date=fromDate, from_date_type=from_date_type, to_date=toDate, to_date_type=to_date_type, record_data=eac_text)

    @classmethod
    def get_all_unprocessed_records(cls, options=None, session=None, limit=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.processed==False)
        if options:
            q = q.options(*options)
        q = q.order_by(cls.id.asc())
        if limit:
            q = q.limit(limit)
        return q.all()
        
    @classmethod
    def get_by_source_id(cls, source_id, options=None, session=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.source_id==source_id)
        if options:
            q = q.options(*options)
        return q.first()
        
    @classmethod
    def get_by_path(cls, path, options=None, session=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.path==path)
        if options:
            q = q.options(*options)
        return q.first()
        
    def name_similar(self, threshold=5, record_groups_only=True, session=None, limit=None, offset=None):
        '''requires the postgres module fuzzystrmatch'''
        if not session:
            session = meta.Session
        q = session.query(self.__class__)
        name_edit_distance = func.levenshtein(self.name_norm, func.substring(self.__class__.name_norm, 1, 254))
        q = q.add_columns(name_edit_distance.label("edit_distance"))
        q = q.filter(name_edit_distance < threshold)
        q = q.filter(self.__class__.id != self.id)
        if record_groups_only:
            q = q.filter(self.__class__.record_group_id != None)
        q = q.order_by(literal_column("edit_distance").asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        return q.all()

    def name_similar_trigram(self, threshold=0.75, record_groups_only=True, session=None, limit=None, offset=None):
        '''requires the postgres module pg_trgm'''
        if not session:
            session = meta.Session
        q = session.query(self.__class__)
        name_trigram_sim = func.similarity(self.name_norm, self.__class__.name_norm)
        q = q.add_columns(name_trigram_sim.label("trigram_sim"))
        q = q.filter(name_trigram_sim > threshold)
        q = q.filter(self.__class__.id != self.id)
        if record_groups_only:
            q = q.filter(self.__class__.record_group_id != None)
        q = q.order_by(literal_column("trigram_sim").desc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        return q.all()
        
class PersonOriginalRecord(OriginalRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_PERSON}
    __basic_attrs__ = ['id', 'source_id', 'name', 'from_date', 'to_date', 'created_at', 'updated_at']
    
class CorporateOriginalRecord(OriginalRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_CORPORATE}
    __basic_attrs__ = ['id', 'source_id', 'name', 'from_date', 'to_date', 'created_at', 'updated_at']
    
class FamilyOriginalRecord(OriginalRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_FAMILY}
    __basic_attrs__ = ['id', 'source_id', 'name', 'from_date', 'to_date', 'created_at', 'updated_at']


class MaybeCandidate(meta.Base, Entity):
    __tablename__ = 'maybe_candidates'
    id = Column(types.BigInteger, primary_key=True)    
    record_group_id = Column(types.BigInteger, ForeignKey('record_groups.id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False, index=True )
    candidate_type = Column(types.Unicode(64), nullable=False)
    candidate_id = Column(types.Unicode(512), nullable=False, index=True)

    @classmethod
    def get_all_by_candidate_id(cls, candidate_id, candidate_type="viaf", options=None, session=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.candidate_id==candidate_id)
        q = q.filter(cls.candidate_type==candidate_type)
        if options:
            q = q.options(*options)
        return q.all()
        

class RecordGroup(meta.Base, Entity):
    __tablename__ = 'record_groups'
    id = Column(types.BigInteger, primary_key=True)
    name = Column(types.Unicode(512), nullable=False, index=True, server_default=u'')
    g_type = Column(types.Unicode(64), nullable=False, index=True)
    viaf_id = Column(types.Unicode(512), nullable=True, index=True, unique=True)
    viaf_record = Column(types.UnicodeText, nullable=True)
    loc_id = Column(types.Unicode(512), nullable=True, index=True, unique=True)
    loc_record = Column(types.UnicodeText, nullable=True)
    created_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, index=True)
    records = orm.relationship("OriginalRecord", backref=orm.backref("record_group", uselist=False))
    merge_records = orm.relationship("MergedRecord", backref=orm.backref("record_group", uselist=False))
    maybe_candidates = orm.relationship("MaybeCandidate", backref=orm.backref("record_group", uselist=False))

    __mapper_args__ = {
        'polymorphic_on':g_type
    }
    
    @classmethod
    def get_by_name(cls, name, options=None, session=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.name==name)
        if options:
            q = q.options(*options)
        return q.first()
    
    @classmethod
    def get_by_viaf_id(cls, viaf_id, options=None, session=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.viaf_id==viaf_id)
        if options:
            q = q.options(*options)
        return q.first()
        
    @classmethod
    def get_by_loc_id(cls, loc_id, options=None, session=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.loc_id==loc_id)
        if options:
            q = q.options(*options)
        return q.first()
        
    @property
    def viaf(self):
        if self.viaf_record:
            return snac2.viaf.getEntityInformation(self.viaf_record)

    @classmethod
    def get_all_with_no_merge_record(cls, session=None, limit=None, offset=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.outerjoin(cls.merge_records)
        q = q.filter(MergedRecord.record_group_id==None)
        q = q.order_by(cls.id.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        return q.all()

        
class PersonGroup(RecordGroup):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_PERSON}
    __basic_attrs__ = ['id', 'name', 'viaf_id', 'created_at', 'updated_at']
    
class CorporateGroup(RecordGroup):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_CORPORATE}
    __basic_attrs__ = ['id', 'name', 'viaf_id', 'created_at', 'updated_at']
    
class FamilyGroup(RecordGroup):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_FAMILY}
    __basic_attrs__ = ['id', 'name', 'viaf_id', 'created_at', 'updated_at']

class MergedRecord(meta.Base, Entity):
    __tablename__ = 'merged_records'
    id = Column(types.BigInteger, primary_key=True)
    canonical_id = Column(types.Unicode(1024), nullable=True, index=True, unique=True)
    name = Column(types.Unicode(255), nullable=False, index=True, server_default=u'')
    r_type = Column(types.Unicode(64), nullable=False, index=True)
    from_date = Column(Date(), nullable=True)
    to_date = Column(Date(), nullable=True)
    record_data = orm.deferred(Column(types.UnicodeText, nullable=True))
    valid = Column(types.Boolean, nullable=False, index=True, server_default="true")
    record_group_id = Column(types.BigInteger, ForeignKey('record_groups.id', onupdate="CASCADE", ondelete="SET NULL"), nullable=True, index=True )
    invalidates_record_id = Column(types.BigInteger, ForeignKey('merged_records.id', onupdate="CASCADE", ondelete="SET NULL"), nullable=True, index=True )
    created_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, index=True)
    invalidated_by = orm.relationship("MergedRecord", backref=orm.backref("invalidates", uselist=False), foreign_keys=[invalidates_record_id], remote_side=[id])

    __mapper_args__ = {
        'polymorphic_on':r_type
    }
    
    def __repr__(self):
        return "<MergedRecord %d %s>" % (self.id, self.name.encode('utf-8'))
    
    @classmethod
    def get_by_canonical_id(cls, canonical_id, options=None, session=None):
        if not session:
            session=meta.Session
        try:
            q = session.query(cls)
            if options:
                q = q.options(*options)
            entity = q.filter(cls.canonical_id == canonical_id).one()
            return entity
        except NoResultFound:
            return None


    @classmethod
    def get_by_record_group_id(cls, record_group_id, options=None, session=None):
        if not session:
            session=meta.Session
        try:
            q = session.query(cls)
            if options:
                q = q.options(*options)
            entity = q.filter(cls.record_group_id == record_group_id).one()
            return entity
        except NoResultFound:
            return None
             
    @classmethod
    def get_all_unassigned(cls, options=None, session=None, limit=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.canonical_id==None)
        if limit:
            q = q.limit(limit)
        if options:
            q = q.options(*options)
        return q.all()
        
    
    def assign_canonical_id(self, is_fake=True):
        ark_id = None
        try:
            ark_id = snac2.noid.get_ark_id(is_fake=is_fake)
        except IOError:
            ark_id = None
        if ark_id:
            self.canonical_id = ark_id
        return ark_id
        
    @classmethod
    def assign_all_canonical_ids_batch(cls, per_batch=1000, last_batch_backup_path="/projects/snac-data/last_ark_ids.pkl", is_fake=True):
        # because ARKs are precious resources and this is prone to network failure, we make a backup of pulled batch first in case we fail - yliu
        ark_ids = []
        if last_batch_backup_path:
            if os.path.exists(last_batch_backup_path):
                ark_ids = pickle.load(open(last_batch_backup_path, "rb"))
                logging.info("Found backups, loaded %d ark_ids" % (len(ark_ids)))
        while True:
            unassigned_records = cls.get_all_unassigned(limit=per_batch)
            n_unassigned = len(unassigned_records)
            if n_unassigned <= 0:
                break
            for i, record in enumerate(unassigned_records):
                while True:
                    if not ark_ids:
                        ark_ids += snac2.noid.get_ark_ids(n_unassigned-i, is_fake=is_fake)
                        logging.info("retrieved %d new ark_ids" % (len(ark_ids)))
                        pickle.dump(ark_ids, open(last_batch_backup_path, "wb"))
                    try:
                        ark_id  = ark_ids[0]
                        record.canonical_id = ark_id
                        logging.info("assigning %d as %s" % (record.id, ark_id))
                        commit()
                        ark_ids.pop(0)
                        break
                    except IntegrityError:
                        rollback()
                        logging.warning("integrity ERROR assigning %d as %s; rolling back" % (record.id, ark_id))
                        ark_ids.pop(0)
            
    
    @classmethod
    def get_by_viaf_id(cls, viaf_id, options=None, session=None):
        if not session:
            session=meta.Session
        try:
            q = session.query(cls)
            q = q.join(cls.record_group)
            if options:
                q = q.options(*options)
            entity = q.filter(RecordGroup.viaf_id == viaf_id).one()
            return entity
        except NoResultFound:
            return None
    
    def get_all_maybes(self):
        candidates = []
        maybe_candidates_from_record_group = self.record_group.maybe_candidates
        for c in maybe_candidates_from_record_group:
            candidate_merged_record = None
            if c.candidate_type == "viaf":
                candidate_merged_record = MergedRecord.get_by_viaf_id(c.candidate_id)
            elif c.candidate_type == "postgres":
                candidate_merged_record = MergedRecord.get_by_record_group_id(int(c.candidate_id))
            if candidate_merged_record and candidate_merged_record.r_type == self.r_type:
                candidates.append(candidate_merged_record)
        if self.record_group.viaf_id:
            maybe_candidates_from_viaf = MaybeCandidate.get_all_by_candidate_id(self.record_group.viaf_id)
            for c in maybe_candidates_from_viaf:
                candidate_merged_record = MergedRecord.get_by_record_group_id(c.record_group_id)
                if candidate_merged_record and candidate_merged_record.id != self.id and candidate_merged_record.r_type == self.r_type:
                    candidates.append(candidate_merged_record)
        return candidates

    @classmethod
    def get_all_assigned_starting_with(cls, id, options=None, session=None, iterate=False, limit=None, offset=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        if options:
            q = q.options(*options)
        q = q.filter(cls.id >= id)
        q = q.filter(cls.canonical_id != None)
        q = q.order_by(cls.id.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        if iterate:
            return q
        return q.all()
    

class PersonMergedRecord(MergedRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_PERSON}
    
class CorporateMergedRecord(MergedRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_CORPORATE}
    
class FamilyMergedRecord(MergedRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_FAMILY}
