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
import snac2.cpf
import snac2.utils as utils

import lxml.etree as etree
import xml.dom.minidom
import cStringIO
from xml.sax.saxutils import escape

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
        

def extractNodeValue(xmlNodeList):
    return xmlNodeList[0].childNodes[0].nodeValue if len(xmlNodeList) > 0 and len(xmlNodeList[0].childNodes) > 0 else None


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
    from_date_type = Column(types.Unicode(255), nullable=True, index=True)
    to_date = Column(Date(), nullable=True)
    to_date_type = Column(types.Unicode(255), nullable=True, index=True)
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
        return "<OriginalRecord %s %s>" % (str(self.id), self.name.encode('utf-8'))
    
    @classmethod
    def load_from_eac(cls, eac_text):
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
        
        #extractNodeValue = lambda xmlNodeList: (xmlNodeList[0].childNodes[0].nodeValue)  if len(xmlNodeList) > 0 and len(xmlNodeList[0].childNodes) > 0 else None
        
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
            genericDateElement = existDate.getElementsByTagName("date")
            fromDate, from_date_type = cls._process_date_element(fromDateElement, default_base=datetime.date(1, 1, 1))
            toDate, to_date_type = cls._process_date_element(toDateElement, default_base=datetime.date(1, 12, 31))
            if genericDateElement and (not fromDate and not toDate):
                fromDate, from_date_type = cls._process_date_element(genericDateElement)
        return record_class(name=names[0], name_norm=utils.normalize_with_space(names[0]), source_id=entityId, from_date=fromDate, from_date_type=from_date_type, to_date=toDate, to_date_type=to_date_type)#, record_data=eac_text)

    @classmethod
    def _process_date_element(cls, element, default_base=None):
        d_type = None
        d = None
        if element and len(element[0].attributes):
            if element[0].attributes.get("localType"):
                d_type = element[0].attributes["localType"].value
            if element[0].attributes.get("standardDate"):
                d = element[0].attributes["standardDate"].value
            else:
                d = extractNodeValue(element)
                if d.startswith("active "):
                    d = d[7:]
            try:
                d = dateutil.parser.parse(d, default=default_base)
            except ValueError:
                d = None
        return d, d_type
    
    @classmethod
    def get_all_unprocessed_records(cls, options=None, session=None, limit=None, min_id=None, max_id=None, collection_id=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.filter(cls.processed==False)
        if collection_id:
            q = q.filter(cls.collection_id==collection_id)
        if min_id:
            q = q.filter(cls.id>=min_id)
        if max_id:
            q = q.filter(cls.id<=max_id)
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
        
    @classmethod
    def get_all_collection_ids(cls, session=None):
        if not session:
            session = meta.Session
        return session.query(cls.collection_id.distinct()).order_by(cls.collection_id);
        
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
    previous_records = orm.relationship("OriginalRecord",
                    secondary="removed_references",
                    backref="previous_groups")
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
    def get_all_with_no_merge_record(cls, session=None, limit=None, offset=None, name_type=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        q = q.outerjoin(cls.merge_records)
        q = q.filter(MergedRecord.record_group_id==None)
        if name_type:
            q = q.filter(cls.g_type == name_type)
        q = q.order_by(cls.id.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        return q.all()
        
    @classmethod
    def get_all_by_type(cls, session=None, limit=None, offset=None, name_type=None, exclude_non_viaf=False, start_at_id=None, end_at_id=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        if name_type:
            q = q.filter(cls.g_type == name_type)
        if exclude_non_viaf:
            q = q.filter(cls.viaf_id != None)
        if start_at_id:
            q = q.filter(cls.id >= start_at_id)
        if end_at_id:
            q = q.filter(cls.id <= end_at_id)
        q = q.order_by(cls.id.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        return q.all()
    
    def reload_viaf(self, viaf_id=None):
        if not viaf_id:
            viaf_id = self.viaf_id
        self.viaf_record = snac2.viaf.query_cheshire_viaf_id(viaf_id)

        
class PersonGroup(RecordGroup):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_PERSON}
    __basic_attrs__ = ['id', 'name', 'viaf_id', 'created_at', 'updated_at']
    
class CorporateGroup(RecordGroup):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_CORPORATE}
    __basic_attrs__ = ['id', 'name', 'viaf_id', 'created_at', 'updated_at']
    
class FamilyGroup(RecordGroup):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_FAMILY}
    __basic_attrs__ = ['id', 'name', 'viaf_id', 'created_at', 'updated_at']
    
class InvalidGroup(RecordGroup):
    __mapper_args__ = {'polymorphic_identity': "invalid"}
    __basic_attrs__ = ['id', 'name', 'viaf_id', 'created_at', 'updated_at']

class MergedRecord(meta.Base, Entity):
    __tablename__ = 'merged_records'
    id = Column(types.BigInteger, primary_key=True)
    canonical_id = Column(types.Unicode(1024), nullable=True, index=True, unique=True)
    name = Column(types.Unicode(1024), nullable=False, index=True, server_default=u'')
    r_type = Column(types.Unicode(64), nullable=False, index=True)
    from_date = Column(Date(), nullable=True)
    to_date = Column(Date(), nullable=True)
    record_data = orm.deferred(Column(types.UnicodeText, nullable=True))
    valid = Column(types.Boolean, nullable=False, index=True, server_default="true")
    processed = Column(types.Boolean, nullable=False, index=True, server_default="false")
    record_group_id = Column(types.BigInteger, ForeignKey('record_groups.id', onupdate="CASCADE", ondelete="SET NULL"), nullable=True, index=True )
    invalidates_record_id = Column(types.BigInteger, ForeignKey('merged_records.id', onupdate="CASCADE", ondelete="SET NULL"), nullable=True, index=True )
    created_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, index=True)
    invalid_reason = Column(types.UnicodeText, nullable=True)
    invalidates = orm.relationship("MergedRecord", backref=orm.backref("invalidated_by"), foreign_keys=[invalidates_record_id], remote_side=[id], uselist=False)
    #invalidated_by_record_id = Column(types.BigInteger, ForeignKey('merged_records.id', onupdate="CASCADE", ondelete="SET NULL"), nullable=True, index=True )
    #last_output_at = Column(DateTime(), nullable=True, index=True)

    __mapper_args__ = {
        'polymorphic_on':r_type
    }
    
    def __repr__(self):
        return "<MergedRecord %d %s>" % (self.id, self.name.encode('utf-8'))
    
    @classmethod
    def get_by_canonical_id(cls, canonical_id, options=None, session=None, only_valid=False):
        if not session:
            session=meta.Session
        try:
            q = session.query(cls)
            if options:
                q = q.options(*options)
            if only_valid:
                q = q.filter(cls.valid == True)
            entity = q.filter(cls.canonical_id == canonical_id).one()
            return entity
        except NoResultFound:
            return None
            
    
#     @classmethod
#     def get_all_by_canonical_ids(cls, canonical_ids, options=None, session=None, only_valid=False, iterate=False):
#         if not session:
#             session = meta.Session
#         if not canonical_ids:
#             return []
#         q = session.query(cls)
#         if options:
#             q = q.options(*options)
#         q = q.filter(cls.canonical_id.in_(canonical_ids))
#         if only_valid:
#             q = q.filter(cls.valid == True)
#         if iterate:
#             return q
#         return q.all()


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
    def get_all_assigned_starting_with(cls, id, end_at=None, force_reprocess=False, options=None, session=None, iterate=False, limit=None, offset=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        if options:
            q = q.options(*options)
        q = q.filter(cls.id >= id)
        q = q.filter(cls.canonical_id != None)
        if end_at:
            q = q.filter(cls.id <= end_at)
        if not force_reprocess:
            q = q.filter(cls.processed == False)  
        q = q.order_by(cls.id.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        if iterate:
            return q
        return q.all()
    
    @classmethod
    def get_all_new_or_changed_since(cls, imported_at, options=None, session=None, iterate=False, limit=None, offset=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        if options:
            q = q.options(*options)
        # TODO: this is potentially a costly query
        q = session.query(cls)
        q = q.join(cls.record_group)
        q = q.join(OriginalRecord, RecordGroup.id ==OriginalRecord.record_group_id)
        q = q.filter(OriginalRecord.record_group_id != None)
        q = q.filter(OriginalRecord.created_at > imported_at)
        q = q.order_by(cls.created_at.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        q = q.distinct()
        if iterate:
            return q
        return q.all()
        
    @classmethod
    def get_all_for_collection_ids(cls, collection_ids, options=None, session=None, iterate=False, limit=None, offset=None):
        if not session:
            session = meta.Session
        q = session.query(cls)
        if options:
            q = q.options(*options)
        # TODO: this is potentially a costly query
        q = session.query(cls)
        q = q.join(cls.record_group)
        q = q.join(OriginalRecord, RecordGroup.id ==OriginalRecord.record_group_id)
        q = q.filter(OriginalRecord.record_group_id != None)
        or_clause = []
        for collection_id in collection_ids:
            or_clause.append(OriginalRecord.collection_id == collection_id)    
        #q = q.filter(OriginalRecord.collection_id.in_(collection_ids))
        q = q.filter(or_(*or_clause))
        q = q.order_by(cls.created_at.asc())
        if limit:
            q = q.limit(limit)
        if offset:
            q = q.offset(offset)
        q = q.distinct()
        if iterate:
            return q
        return q.all()
    
    def to_cpf(self):
        logging.info("creating output for %d: %s" % (self.record_group.id, self.canonical_id))
        if self.valid:
            combined_record = self.create_combined_record()
        else:
            combined_record = self.create_tombstone_record()
        #print combined_record
        try:
            doc = xml.dom.minidom.parseString(combined_record)
        except Exception, e:
            doc = None
            logging.warning(e)
            logging.warning( "Failed record follows" )
            logging.warning( combined_record )
        return doc
    
    def create_tombstone_record_from_stored(self):
        # not used; human validation only
        cpf = self.record_data
        if cpf:
            doc = xml.dom.minidom.parseString(cpf.encode('utf-8'))
            maintenanceNode = doc.getElementsByTagName("maintenanceStatus")[0]
            maintenanceNode.firstChild.replaceWholeText("cancelled")
            cancelEventNode = utils.minidom_create_element(doc, "maintenanceEvent")
            eventTypeNode = utils.minidom_create_element(doc, "eventType", text="cancelled")
            cancelEventNode.appendChild(eventTypeNode)
            if self.invalid_reason:
                eventTypeNode = utils.minidom_create_element(doc, "eventDescription", text=self.invalid_reason)
            if self.invalidated_by:
                if len(self.invalidated_by) == 1:
                    otherRecordNode = utils.minidom_create_element(doc, "otherRecordId", attrs={"localType":"http://socialarchive.iath.virginia.edu/control/term#MergedInto"}, text=self.invalidated_by[0].canonical_id)
                    cancelEventNode.appendChild(otherRecordNode)
                elif len(self.invalidated_by) > 1:
                    for r in self.invalidated_by:
                        otherRecordNode = utils.minidom_create_element(doc, "otherRecordId", attrs={"localType":"http://socialarchive.iath.virginia.edu/control/term#SplitInto"}, text=r.canonical_id)
                        cancelEventNode.appendChild(otherRecordNode)
            historyNode =  doc.getElementsByTagName("maintenanceHistory")[0]
            previousEventNode = doc.getElementsByTagName("maintenanceEvent")[0]
            historyNode.insertBefore(cancelEventNode, previousEventNode)
            return doc.toxml().encode('utf-8')
        else:
            return None
            
    def create_tombstone_record(self):
        # quick hack to generate shortened tombstones
        base = u'''<?xml version="1.0" encoding="UTF-8"?>
<?xml-model href="http://socialarchive.iath.virginia.edu/shared/cpf.rng" type="application/xml" schematypens="http://relaxng.org/ns/structure/1.0"?>
<eac-cpf xmlns="urn:isbn:1-931666-33-4">
    <control>
        <recordId> </recordId>
        <maintenanceStatus>cancelled</maintenanceStatus>
        <maintenanceAgency>
            <agencyName>SNAC: Social Networks and Archival Context Project</agencyName>
        </maintenanceAgency>
        <maintenanceHistory>
            <maintenanceEvent>
                <eventType>cancelled</eventType>
                <eventDateTime> </eventDateTime>
                <agentType>machine</agentType>
                <agent>CPF merge program</agent>
            </maintenanceEvent>
        </maintenanceHistory>
    </control>
    <cpfDescription>
        <identity>
            <entityType> </entityType>
            <nameEntry><part> </part></nameEntry>
        </identity>
    </cpfDescription>
</eac-cpf>'''
        doc = xml.dom.minidom.parseString(base)
        recordIdNode = doc.getElementsByTagName("recordId")[0]
        recordIdNode.firstChild.replaceWholeText(self.canonical_id)
        entityTypeNode = doc.getElementsByTagName("entityType")[0]
        entityTypeNode.firstChild.replaceWholeText(self.r_type)
        dateTimeNode = doc.getElementsByTagName("eventDateTime")[0]
        dateTimeNode.firstChild.replaceWholeText(str(datetime.date.today()))
        partNode = doc.getElementsByTagName("part")[0]
        if self.record_group.previous_records:
            partNode.firstChild.replaceWholeText(self.record_group.previous_records[0].name)
        if not self.valid:
            if self.invalidated_by:
                if len(self.invalidated_by) == 1:
                    otherRecordNode = utils.minidom_create_element(doc, "otherRecordId", attrs={"localType":"http://socialarchive.iath.virginia.edu/control/term#MergedInto"}, text=self.invalidated_by[0].canonical_id)
                    recordIdNode.parentNode.insertBefore(otherRecordNode, recordIdNode.nextSibling);
                elif len(self.invalidated_by) > 1:
                    for record in self.invalidated_by:
                        otherRecordNode = utils.minidom_create_element(doc, "otherRecordId", attrs={"localType":"http://socialarchive.iath.virginia.edu/control/term#SplitInto"}, text=self.invalidated_by[0].canonical_id)
                        recordIdNode.parentNode.insertBefore(otherRecordNode, recordIdNode.nextSibling);
        if self.invalidates:
            if len(self.invalidates.invalidated_by) == 1:
                otherRecordNode = utils.minidom_create_element(doc, "otherRecordId", attrs={"localType":"http://socialarchive.iath.virginia.edu/control/term#MergedFrom"}, text=self.invalidated_by[0].canonical_id)
                recordIdNode.parentNode.insertBefore(otherRecordNode, recordIdNode.nextSibling);
                #cr.write('<otherRecordId localType="%s">%s</otherRecordId>' % ("http://socialarchive.iath.virginia.edu/control/term#MergedFrom", self.invalidates.canonical_id))
            elif len(self.invalidated_by) > 1:
                otherRecordNode = utils.minidom_create_element(doc, "otherRecordId", attrs={"localType":"http://socialarchive.iath.virginia.edu/control/term#SplitFrom"}, text=self.invalidated_by[0].canonical_id)
                recordIdNode.parentNode.insertBefore(otherRecordNode, recordIdNode.nextSibling);
                #cr.write('<otherRecordId localType="%s">%s</otherRecordId>' % ("http://socialarchive.iath.virginia.edu/control/term#SplitFrom", self.invalidates.canonical_id))
        return doc.toxml()
        
    def create_combined_record(self):
        # drawn from code formerly in merge.py
        # TODO: still need to rewrite this to use a real parser instead of writing out strings.
        # TODO: and get rid of minidom
        cpfRecords = []
        if self.valid:
            cpfRecords = [record.path for record in self.record_group.records]
        else:
            cpfRecords = [record.path for record in self.record_group.previous_records]
        viaf_info = snac2.viaf.VIAF.get_empty_viaf_dict()
        if self.record_group.viaf_record:
            viaf_info = snac2.viaf.getEntityInformation(self.record_group.viaf_record)
        r_type = self.r_type
        canonical_id = self.canonical_id
        maybes = self.get_all_maybes()
        #record id
        mrecordIds = []
    
        #type
        mtypes = []
    
        #names
        mnames = []
    
        #sources
        msources = []
    
        #exist dates
        mexistDates = []
    
        #occupations
        moccupations = []
    
        #local descriptions
        mlocalDescriptions = []
    
        #relations
        mrelations = []
    
        #resource relations
        mresourceRelations = []
    
        #biography
        mbiography = []
    
        #function
        mfunctions = []
        
        mplaces = []
        
        mlanguages = []
        
        mdescriptionmisc = []
        
        mentityids = []
    
        relations_canonical_idx = {}
        relations_role_and_href_idx = {}
        
        for cpfRecord in cpfRecords:
            legacyDoc = xml.dom.minidom.parse(cpfRecord)
            try:
                etree_doc = etree.XML(open(cpfRecord).read())
            except etree.XMLSyntaxError, e:
                p = etree.XMLParser(huge_tree=True) # retry the parse with a huge_tree
                etree_doc = etree.parse(cpfRecord, parser=p)
                logging.warning("WARNING having trouble with %s, falling back to huge_tree parser" % (cpfRecord))
            except Exception, e:
                logging.error("ERROR failed to parse %s" % (cpfRecord))
                raise e
            identityInfo = snac2.cpf.parseIdentity(legacyDoc)
            if identityInfo != None:
                recordIds = identityInfo['id']
                if recordIds != None:
                    mrecordIds.append(recordIds)         
                names =  identityInfo['name_entries']
                if names != None:
                    mnames.extend(names)
                type =  identityInfo['type']
                if type != None:
                    mtypes.append(type)
            
            entityId = snac2.cpf.parseEntityId(legacyDoc)
            if entityId != None:
                mentityids.append(entityId)
        
            sources = snac2.cpf.parseSources(legacyDoc)
            if sources != None:
                msources.extend(sources)
        
            existDates = snac2.cpf.parseExistDates(legacyDoc)
            if existDates != None:
                mexistDates.extend(existDates)
        
            occupations = snac2.cpf.parseOccupations(legacyDoc)
            if occupations != None:
                moccupations.extend(occupations)
                
            languages = snac2.cpf.parseLanguageUsed(legacyDoc)
            if languages:
                mlanguages.extend(languages)
        
            localDescriptions = snac2.cpf.parseLocalDescriptions(legacyDoc)
            if localDescriptions != None:
                mlocalDescriptions.extend(localDescriptions)
            
            places = snac2.cpf.parsePlaces(legacyDoc)
            mplaces.extend(places)
            
            functions = snac2.cpf.parseFunctions(legacyDoc)
            if functions:
                mfunctions.extend(functions)
            
            descriptions = snac2.cpf.parseDescriptions(legacyDoc)
            if descriptions:
                mdescriptionmisc.extend(descriptions)
        
            relations = snac2.cpf.parseAssociationsRaw(legacyDoc)
            if relations != None:
                filtered_relations = []
                for relation in relations:
                    seen = False
                    # detect whether there is an internal SNAC relation to dereference
                    extracted_records = relation.getElementsByTagName("span")
                    if extracted_records:
                        extracted_records = extracted_records[0]
                        extracted_record_id = extracted_records.childNodes[0].nodeValue
                        extracted_record_id = extracted_record_id.strip()
                        #print "EXTRACTED SOURCE ID %s from %s" % (extracted_record_id, cpfRecord)
                        if extracted_record_id.startswith("recordId: "):
                            extracted_record_id = extracted_record_id[len("recordId: "):] # correct for extraneous henry_cpf prefixes
                        original_record = OriginalRecord.get_by_source_id(extracted_record_id)
                        if original_record:
                            record_group = original_record.record_group
                            if not record_group:
                                logging.warning("Warning %s has no record_group" % (extracted_record_id))
                                relation.setAttribute("xlink:href", "")
                                continue
                            merge_records = record_group.merge_records
                            if not merge_records:
                                logging.warning("Warning %s has no merge record" % (extracted_record_id))
                                relation.setAttribute("xlink:href", "")
                                continue
                            for record in merge_records:
                                if record.canonical_id in relations_canonical_idx:
                                    seen = True # there is a duplicate
                                    #logging.info("%s already seen" % (record.canonical_id))
                                    continue
                                else:
                                    relations_canonical_idx[record.canonical_id] = 1 # make sure no duplicates
                                    #logging.info( "%s recorded" % (record.canonical_id) )
                                if record.valid:
                                    if record.canonical_id:
                                    
                                        relation.setAttribute("xlink:href", record.canonical_id)
                                    else:
                                        logging.warning( "Warning %s has no ARK ID" % (extracted_record_id) )
                                        relation.setAttribute("xlink:href", "")
                                    break
                                else:
                                    logging.warning( "record not valid" )
                        else:
                            logging.warning( "Warning %s has no original record" % (extracted_record_id) )
                        desc = relation.getElementsByTagName("descriptiveNote")
                        if desc:
                            desc = desc[0]
                            spans = desc.getElementsByTagName("span")
                            for span in spans:
                                if (span.attributes.get("localType") and span.attributes.get("localType") == "snac:extractRecordId"):
                                    relation.removeChild(desc)
                        # relation.removeChild(relation.getElementsByTagName("descriptiveNote")[0])
                    else:
                        # in this case, we assume there is a xlink:href already, probably external
                        href = relation.attributes.get("xlink:href")
                        if not href:
                            logging.warning("Failed to see a xlink:href in %s and no descriptiveNote either" % (cpfRecord))
                        else:
                            # if there is an href, check whether its role and href are the same.  if so, it needs duplicate-checking
                            role = relation.attributes.get("xlink:arcrole")
                            if role:
                                if relations_role_and_href_idx.get(role.value+"|"+href.value):
                                    seen = True
                                else:
                                    relations_role_and_href_idx[role.value+"|"+href.value] = 1
                        # otherwise just passthrough
                    if not seen:
                        filtered_relations.append(relation)
                mrelations.extend(filtered_relations)
        
            resourceRelations = snac2.cpf.parseResourceAssociationsRaw(legacyDoc)
            if resourceRelations != None:
                mresourceRelations.extend(resourceRelations)
            
            biography = snac2.cpf.parseBiogHist2(etree_doc)
            if biography != None:
                mbiography.append(biography)
            
    
        #Stich a new record
        cr = cStringIO.StringIO()
    
        #Root
        cr.write('<?xml version="1.0" encoding="UTF-8"?>')
        cr.write("""<eac-cpf xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:owl="http://www.w3.org/2002/07/owl#"
             xmlns:snac="http://socialarchive.iath.virginia.edu/"
             xmlns="urn:isbn:1-931666-33-4"
             xmlns:xlink="http://www.w3.org/1999/xlink">""")
         
        #Control
        cr.write("<control>")
    
        #RecordID
        i = 0
        if canonical_id:
            cr.write('<recordId>%s</recordId>' % canonical_id)

        if not self.valid:
            if self.invalidated_by:
                if len(self.invalidated_by) == 1:
                    cr.write('<otherRecordId localType="%s">%s</otherRecordId>' % ("http://socialarchive.iath.virginia.edu/control/term#MergedInto", self.invalidated_by[0].canonical_id))
                elif len(self.invalidated_by) > 1:
                    for record in self.invalidated_by:
                        cr.write('<otherRecordId localType="%s">%s</otherRecordId>' % ("http://socialarchive.iath.virginia.edu/control/term#SplitInto", record.canonical_id))
        if self.invalidates:
            if len(self.invalidates.invalidated_by) == 1:
                cr.write('<otherRecordId localType="%s">%s</otherRecordId>' % ("http://socialarchive.iath.virginia.edu/control/term#MergedFrom", self.invalidates.canonical_id))
            elif len(self.invalidated_by) > 1:
                cr.write('<otherRecordId localType="%s">%s</otherRecordId>' % ("http://socialarchive.iath.virginia.edu/control/term#SplitFrom", self.invalidates.canonical_id))
        for recordId in mrecordIds:
            cr.write('<otherRecordId localType="http://socialarchive.iath.virginia.edu/control/term#MergedRecord">%s</otherRecordId>' %recordId)
            i = i + 1
        
        #VIAF
        #1. From VIAF, add to eac-cpf/control after recordId
        viafRecordId = viaf_info['recordId']
        if viafRecordId != None:
            cr.write('<otherRecordId localType="http://viaf.org/viaf/terms#viafID">')
            cr.write('%s' %viafRecordId)
            cr.write('</otherRecordId>') 

        #DBPedia
        #From DBPedia, add to eac-cpf/control after recordid
        #dbpediaRecordId = dbpediaInfo['recordId']
        #if dbpediaRecordId != None:
        #    cr.write('<otherRecordId localType="dbpedia">')
        #    cr.write('dbpedia:%s' %dbpediaRecordId)
        #    cr.write('</otherRecordId>')

        #Maintenance
        if self.valid:
            cr.write("<maintenanceStatus>revised</maintenanceStatus>")
        else:
            cr.write("<maintenanceStatus>cancelled</maintenanceStatus>")
        cr.write("""<maintenanceAgency>
                         <agencyName>SNAC: Social Networks and Archival Context Project</agencyName>
                    </maintenanceAgency>""")
    
        #Language
        cr.write("""<languageDeclaration>
                    <language languageCode="eng">English</language>
                    <script scriptCode="Latn">Latin Alphabet</script>
                    </languageDeclaration>""")
    
        #2.From VIAF, add to eac-cpf/control after languageDeclaration
        if viafRecordId != None:
            cr.write('<conventionDeclaration>')
            cr.write('<citation>VIAF</citation>')
            cr.write('</conventionDeclaration>')

    
        #Maintenance History
        if self.valid:
            cr.write("""<maintenanceHistory>
                        <maintenanceEvent>
                        <eventType>revised</eventType>
                        <eventDateTime standardDateTime=\"%s\">%s</eventDateTime> 
                        <agentType>machine</agentType>
                        <agent>CPF merge program</agent>
                        <eventDescription>Merge v2.0</eventDescription>
                        </maintenanceEvent>
                        </maintenanceHistory>""" % (datetime.datetime.today().isoformat(), datetime.datetime.today()))
        else:
            cr.write("""<maintenanceHistory>
             <maintenanceEvent>
            <eventType>cancelled</eventType>
            <eventDateTime standardDateTime=\"%s\">%s</eventDateTime> 
            <agentType>machine</agentType>
            <agent>CPF merge program</agent>
            <eventDescription>%s</eventDescription>
            </maintenanceEvent>
            </maintenanceHistory>""" % (datetime.datetime.today().isoformat(), datetime.datetime.today(), self.invalid_reason if self.invalid_reason else ""))
        
        #Sources
        if msources:
            cr.write("<sources>")
            for source in msources:
                if source:
                    # NOTE: if the code blows up here with:
                    # data = data.replace("&", "&amp;").replace("<", "&lt;")
                    # AttributeError: 'NoneType' object has no attribute 'replace'
                    # there is a bug in Python 2.6's minidom (of course there is. why are we using it again?). See http://bugs.python.org/issue5762.  Upgrade to Python 2.7 or monkeypatch it.
                    cr.write(source.toxml(encoding='utf-8')) 

            #VIAF
            if viafRecordId != None:
                cr.write('<source xlink:type="simple" xlink:href="http://viaf.org/viaf/%s"/>' %viafRecordId)
    
            cr.write("</sources>")
    
        #END control
        cr.write("</control>")
    
        #CPF Description
        cr.write("<cpfDescription>")
    
        #Identity
        cr.write("<identity>")
    
        #Type
        for type in set(mtypes):
            cr.write("<entityType>%s</entityType>" %type)
        
        for entityId in mentityids:
            cr.write(entityId.toxml().encode('utf-8'))
            
        #Name
        cpf_names = {}
        for k, val in enumerate(mnames):
            cpf_names[k] = val
        #print cpf_names
        names = merge_name_entries(viaf_info['authForms'], viaf_info['altForms'], cpf_names)
        name_tuples = names.items()
        name_tuples.sort(key=lambda x: x[1].preferenceScore, reverse=True)
        authorized_sources = set(["lc", "lac", "nla", 'oac'])
        for name_tuple in name_tuples:
            cr.write('<nameEntry snac:preferenceScore="%s"%s%s>' % (str(name_tuple[1].preferenceScore).zfill(2),
                                                                      ' xml:lang=\"' + name_tuple[1].lang + '\"'   if name_tuple[1].lang else "",
                                                                      ' scriptCode=\"' + name_tuple[1].script_code + '\"'   if name_tuple[1].script_code else "",
                                                                )
                    )
            cr.write('<part>')
            cr.write(escape(name_tuple[1].name).encode('utf-8'))
            cr.write('</part>\n')
            viaf_auth_written = False
            name_sources = name_tuple[1].sources
            for k in name_sources:
                source = name_sources[k] 
                if source['n_type'] == utils.NAME_ENTRY_AUTHORIZED:
                    if source["source"].lower() in authorized_sources or source['name_origin'] == "cpf":
                        cr.write('<authorizedForm>')
                        cr.write(source["source"])
                        cr.write('</authorizedForm>\n')
                    elif not viaf_auth_written:
                        cr.write('<authorizedForm>VIAF</authorizedForm>\n')
                        viaf_auth_written = True
                else:
                    if source["source"] == "VIAF" and viaf_auth_written:
                        pass
                    else:
                        cr.write('<alternativeForm>')
                        cr.write(source["source"])
                        cr.write('</alternativeForm>\n')
            if name_tuple[1].use_dates:
                use_date = name_tuple[1].use_dates[0]
                cr.write(use_date.toxml().encode('utf-8'))
            cr.write('</nameEntry>\n')
        
        #END identity   
        cr.write("</identity>")
    
        if self.valid:
            #Description
            cr.write("<description>")
        
            #Exist Dates
    
            existDate = viaf_info['dates']
            if existDate != None and (existDate[0] !='0' or existDate[1] != '0'):
                cr.write("<existDates>")
                cr.write("<dateRange>")
                if existDate[0] and existDate[0] != '0':
                    term_start = "Birth"
                    if r_type != "person":
                        term_start = "Active"
                    cr.write("<fromDate standardDate=\"%s\" localType=\"http://socialarchive.iath.virginia.edu/control/term#%s\">" % (snac2.cpf.pad_exist_date(existDate[0]), term_start))
                    cr.write(escape(existDate[0]))
                    cr.write("</fromDate>")
                if existDate[1] and existDate[1] != '0':
                    term_end = "Death"
                    if r_type != "person":
                        term_start = "Active"
                    cr.write("<toDate standardDate=\"%s\" localType=\"http://socialarchive.iath.virginia.edu/control/term#%s\">" % (snac2.cpf.pad_exist_date(existDate[1]), term_end))
                    cr.write(escape(existDate[1]))
                    cr.write("</toDate>")
                cr.write("</dateRange>")
                cr.write("</existDates>")
            else:
                if len(mexistDates) > 0:
                    bestExistDate = mexistDates[0]
                    #for existDate in mexistDates[1:]:
                    #    if (is_better_existence(bestExistDate, existDate)):
                    #        bestExistDate = existDate
                    cr.write(bestExistDate.toxml().encode('utf-8')) 
                else:
                    #logging.info("No exist dates")
                    pass
            
            #Languages used from VIAF - should add script
            entityLanguages = viaf_info['language']
            for language in set(entityLanguages):
                cr.write('<languageUsed>')
                cr.write('<language languageCode="%s"/>' % escape(language.lower()).encode('utf-8'))
                cr.write('<script scriptCode="Zyyy"/>')
                cr.write('</languageUsed>')
        
            if mlanguages:
                for language in mlanguages:
                    languageCode = language.getElementsByTagName("language")
                    if languageCode:
                        languageCode = languageCode[0]
                        if languageCode.attributes.get("languageCode").value.strip() not in entityLanguages:
                            entityLanguages.append(languageCode.attributes.get("languageCode").value.strip())
                            cr.write(language.toxml().encode('utf-8'))
                        
            # functions
            function_terms = {}
            for function in mfunctions:
                if function.getElementsByTagName("term"):
                    term = snac2.cpf.extract_subelement_content_from_entry(function, "term")
                    localType = function.attributes.get("localType")
                    if localType:
                        localType = localType.value
                    if term not in function_terms:
                        function_terms[term] = (function, localType)
                    elif function_terms[term][1] != None and not localType:
                        function_terms[term] = (function, localType)
                else:
                    # just passthrough since we're not uniquefying
                    cr.write(function.toxml().encode('utf-8'))
            function_terms_items = function_terms.items()
            for item in function_terms_items:    
                cr.write(item[1][0].toxml().encode('utf-8'))
    
            #Local Descriptions
            subjects = {}
            places = {} # placeEntry from <place> tags 
            desc_places = {} # placeEntry from <localDescription> tags; think this is obsolete
            misc = []
    
            num_and_spaces_re = re.compile(r"^[0-9 -]+$")
            for localDescription in mlocalDescriptions:
                localType = localDescription.attributes.get("localType")
                if localType:
                    localType = localType.value
                if localType == "http://socialarchive.iath.virginia.edu/control/term#AssociatedSubject":
                    subject = snac2.cpf.extract_subelement_content_from_entry(localDescription, "term")
                    if num_and_spaces_re.match(subject):
                        continue
                    else:
                        subjects[subject] = subjects.get(subject, 0) + 1
                elif localType == "http://socialarchive.iath.virginia.edu/control/term#AssociatedPlace":
                    # TODO: I think this is no longer used after <place> was introduced, but left here for historical reasons -yl
                    place = snac2.cpf.extract_subelement_content_from_entry(localDescription, "placeEntry")
                    desc_places[place] = desc_places.get(place, 0) + 1
                else:
                    misc.append(localDescription)
                        #cr.write(localDescription.toxml().encode('utf-8'))

            subjects_list = subjects.items()
            subjects_list.sort(key=lambda x: x[1], reverse=True)
            desc_places_list = desc_places.items()
            desc_places_list.sort(key=lambda x: x[1],  reverse=True)
            for subject_item in subjects_list:
                cr.write('<localDescription localType="http://socialarchive.iath.virginia.edu/control/term#AssociatedSubject">')
                cr.write('<term>%s</term>' % escape(subject_item[0]).encode('utf-8'))
                cr.write('</localDescription>\n')
            for place_item in desc_places_list:
                cr.write('<localDescription localType="http://socialarchive.iath.virginia.edu/control/term#AssociatedPlace">')
                cr.write('<placeEntry>%s</placeEntry>' % escape(place_item[0]).encode('utf-8'))
                cr.write('</localDescription>\n')
            for place in mplaces:
                cr.write(place.toxml().encode('utf-8'))
                cr.write("\n")
            for item in misc:
                cr.write(item.toxml().encode('utf-8'))
        
            #Nationality from VIAF
            entityNationality = viaf_info['nationality']
            for nationality in set(entityNationality):
                cr.write('<localDescription localType="http://viaf.org/viaf/terms#nationalityOfEntity">')
                if not nationality.isupper() or not nationality.isalpha():
                    cr.write('<term>%s</term>' % escape(nationality).encode('utf-8'))
                else:
                    cr.write('<placeEntry countryCode="%s"/>' % escape(nationality).encode('utf-8'))
                cr.write('</localDescription>')
        
            #Gender from VIAF
            gender = viaf_info['gender']
            if gender != None:
                cr.write('<localDescription localType="http://viaf.org/viaf/terms#gender">')
                cr.write('<term>%s</term>' %gender)
                cr.write('</localDescription>')
        
        
            #Occupations
            if moccupations:
                #print moccupations
                occupations_index = set()
                moccupations = filter(lambda o: snac2.cpf.extract_subelement_content_from_entry(o, "term") not in occupations_index and (occupations_index.add(snac2.cpf.extract_subelement_content_from_entry(o, "term")) or True), moccupations)

            for occupation in moccupations:
                cr.write(occupation.toxml().encode('utf-8'))

            #BiogHist
            biogText = {}
            biogData = []
            #chronlists = []
            for biogHist in mbiography:
                if biogHist.get('citation') and biogHist.get('text'):
                    text = biogHist['text']
                    citation = biogHist['citation']
                    if text in biogText:
                        biogText[text]['citation'].append(citation)
                    else:
                        biogText[text] = {'text':text, 'citation':[citation]}
                elif biogHist.get('data'):
                    biogData.append(biogHist['data'])
            #print mbiography
            #print biogText
            if biogText or biogData:
                cr.write("<biogHist>")
                for text in biogText.keys():
                    cr.write("%s" % text.encode('utf-8'))
                    for citation in biogText[text]['citation']:
                        if citation:
                            cr.write("%s" % citation.encode('utf-8')) # do not escape
                for data in biogData:
                    cr.write("%s" % data.encode('utf-8'))
                cr.write("</biogHist>")
            
            if mdescriptionmisc:
                for description in mdescriptionmisc:
                    for k in description.keys():
                        for v in description[k]:
                            cr.write(v.toxml().encode('utf-8'))
                
            #END Description
            cr.write("</description>")
    
            #Relations
            cr.write("<relations>")
    
            #CPF Relations
            for relation in mrelations:
                #print [(k, relation.attributes[k].value) for k in relation.attributes.keys()]
                cr.write(relation.toxml().encode('utf-8'))
        
            if r_type == "person" or r_type == "corporateBody":
                r_type_t = r_type.capitalize()
                headings = viaf_info['mainHeadings']
                if headings:
                    cr.write('<cpfRelation xlink:type="simple" xlink:href="http://viaf.org/viaf/%s"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s">' % (viafRecordId, r_type_t)) 
                    cr.write("<relationEntry>%s</relationEntry>" % (escape(headings[0]).encode('utf-8')))
                    cr.write("</cpfRelation>")
                headingsEl = viaf_info['mainElementEl']
                if headingsEl:
                    for h in headingsEl:
                        if h["source"].startswith("LC"):
                            cr.write('<cpfRelation xlink:type="simple"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs" xlink:href="http://www.worldcat.org/wcidentities/%s" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s" />' % (h["lccn_wcid"], r_type_t)) 
                            cr.write('<cpfRelation xlink:type="simple"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs" xlink:href="http://id.loc.gov/authorities/names/%s" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s" />' % (h["lccn_lcid"], r_type_t)) 
                        elif h["source"].startswith("WKP"):
                            cr.write((u'<cpfRelation xlink:type="simple" xlink:href="http://en.wikipedia.org/wiki/%s"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs"  xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s" />' % (urllib.quote(h["url_id"].encode('utf-8')), r_type_t)).encode('utf-8')) 

            if maybes:
                for merge_candidate in maybes:
                    #print merge_candidate.record_group.records[0].name.__repr__()
                    if (merge_candidate.canonical_id and merge_candidate.record_group.records and merge_candidate.record_group.records[0].name):
                        cr.write('<cpfRelation xlink:type="simple"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#mayBeSameAs" xlink:href="%s" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s"><relationEntry>%s</relationEntry></cpfRelation>' % (merge_candidate.canonical_id.encode('utf-8'), r_type_t.encode('utf-8'), escape(merge_candidate.record_group.records[0].name).encode('utf-8'))) 
                    else:
                        raise ValueError("Problem exporting maybe merge candidate %d: missing canonical_id, name.", merge_candidate.id)
        
            #Resource Relations
            for resourceRelation in mresourceRelations:
                cr.write(resourceRelation.toxml().encode('utf-8'))
        
            #Titles from VIAF
            for title in viaf_info['titles']:
                cr.write('<resourceRelation xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#creatorOf" xlink:role="http://socialarchive.iath.virginia.edu/control/term#BibliographicResource" xlink:type="simple">')
                cr.write('<relationEntry>')
                title = title.replace("&", "&amp;").replace("<", "&lt;")
                title = title.replace("\"", "&quot;").replace(">", "&gt;")
                title = title.replace("\r", "&#xD;").replace("\n", "&#xA;")
                title = title.replace("\t", "&#x9;")


                cr.write(escape(title).encode('utf-8'))
                cr.write('</relationEntry>')
                cr.write('</resourceRelation>')

            
            #END Relations
            cr.write("</relations>")   
    
        #END CPF Description
        cr.write("</cpfDescription>")
    
        #END EAC
        cr.write("</eac-cpf>")
    
        return cr.getvalue()
    
    def split_record(self, id_groups, reason=None):
        '''id_groups is a list of lists, containing original_record.ids for each group to split out'''
        if not self.record_data:
            # try to save a copy of the cpf
            doc = self.to_cpf()
            if doc:
                self.record_data = doc.toxml()
        # set original records from this record_group to the new record_groups
        if self.record_group.records:
            # ensure there is a formatted name
            self.name = self.record_group.records[0].name
        new_record_groups = []
        for group in id_groups:
            record_group = self.record_group.__class__()
            for id in group:
                record = OriginalRecord.get_by_id(id)
                if not record:
                    logging.warn("%d isn't a valid record id to split" % (id))
                if not record_group.name:
                    record_group.name = record.name_norm
                record_group.save()
                record_group.records.append(record) # this is a SQLAlchemy dynamic collection that automatically reassigns the foreign key.  in theory.
                self.record_group.previous_records.append(record)
            new_record_groups.append(record_group)
        flush() # make sure we have ids to work with
        # set the old record_group to invalid, and set invalidated_by
        self.valid = False
        for record_group in new_record_groups:
            merged_record = MergedRecord(r_type=record_group.g_type, name=record_group.name, record_data="", valid=True)
            merged_record.save()
            merged_record.record_group_id = record_group.id
            merged_record.invalidates_record_id = self.id
        self.invalid_reason = reason
        commit()
        return new_record_groups
        
    def absorb_record(self, target_merged_record, reason=None):
        '''given another merged_record, take over all the original_records it has, record the final tombstone, and redirect the target to this one'''
        target_rg = target_merged_record.record_group
        if not target_rg:
            logging.warning("target_merged_record %d does not have a proper record_group and seems to be invalid" % (target_merged_record.id))
            return None
        final_cpf = target_merged_record.to_cpf()
        if not final_cpf:
            logging.warning("Could not generate final CPF for MergedRecord %d.  aborting." % (target_merged_record.id))
            return None
        target_merged_record.record_data = final_cpf.toxml()
        flush()
        for record in target_rg.records:
            record.record_group_id = self.record_group.id
            target_rg.previous_records.append(record)
            #self.record_group.records.append(record)
        flush()
        target_merged_record.valid = False
        self.invalidates = target_merged_record
        target_merged_record.invalid_reason = reason
        commit()
        return self
        
        
def merge_name_entries(viaf_auths, viaf_alts, cpf_identities):
    merged_names = {}
    for name_dict in [viaf_auths, viaf_alts, cpf_identities]:
        name_origin = "viaf_auth"
        if name_dict == viaf_alts:
            name_origin = "viaf_alts"
        elif name_dict == cpf_identities:
            name_origin = "cpf"
        for k in name_dict.keys():
            name_entry = name_dict[k]
            name_norm = name_entry.name_norm
            if name_norm in merged_names:
                merged_names[name_norm].merge(name_entry, name_origin=name_origin)
            else:
                merged_names[name_norm] = utils.MergedNameEntry(name=name_entry.name, name_norm=name_norm, sources={}, lang=name_entry.lang, script_code=name_entry.script_code, use_dates=name_entry.use_dates)
                merged_names[name_norm].merge_sources(name_entry, name_origin=name_origin)
    return merged_names
    
def is_better_existence(original, new):
    original_from = original.getElementsByTagName("fromDate")
    original_to = original.getElementsByTagName("toDate")  
    new_from = new.getElementsByTagName("fromDate")
    new_to = new.getElementsByTagName("toDate")
    if original_from and new_from:
        original_from_date = original_from[0].attributes["standardDate"].value
        new_from_date = new_from[0].attributes["standardDate"].value
        original_from_type = original_from[0].attributes["localType"].value
        new_from_type = new_from[0].attributes["localType"].value
        if new_from_type == RECORD_DATE_TYPE_BIRTH and (not original_from_type or original_from_type == RECORD_DATE_TYPE_ACTIVE):
             return True
        elif new_from_type is not None and new_from_type == original_from_type:
            if len(original_from_date) < len(new_from_date):
                return True
    elif original_to and new_to:
        original_to_date = original_to[0].attributes["standardDate"].value
        new_to_date = new_to[0].attributes["standardDate"].value
        original_to_type = original_to[0].attributes["localType"].value
        new_to_type = new_to[0].attributes["localType"].value
        if new_to_type == RECORD_DATE_TYPE_DEATH and (not original_from_type or original_from_type == RECORD_DATE_TYPE_ACTIVE):
             return True
        elif new_to_type is not None and new_to_type == original_to_type:
            if len(original_to_date) < len(new_to_date):
                return True
    return False

class PersonMergedRecord(MergedRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_PERSON}
    
class CorporateMergedRecord(MergedRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_CORPORATE}
    
class FamilyMergedRecord(MergedRecord):
    __mapper_args__ = {'polymorphic_identity': RECORD_TYPE_FAMILY}

class RemovedReference(meta.Base, Entity):
    __tablename__ = 'removed_references'
    id = Column(types.BigInteger, primary_key=True)    
    original_record_id = Column(types.BigInteger, ForeignKey('original_records.id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False, index=True )
    record_group_id = Column(types.BigInteger, ForeignKey('record_groups.id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False, index=True )
    created_at = Column(DateTime(), nullable=False, default=datetime.datetime.utcnow)
