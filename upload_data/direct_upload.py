import json
import numpy as np

import pymongo
from pymongo import MongoClient

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()
class Street(Base):
    __tablename__ = 'streets'
    id_ = Column(Integer,primary_key=True)
    city_id = Column(Integer)
    name = Column(String)
    length = Column(Float)

class Issue(Base):
    __tablename__ = 'issues'
    id_ = Column(Integer,primary_key=True)
    city_id = Column(Integer)
    request_type_id = Column(Integer)
    street_id = Column(Integer)
    created_at = Column(String)
    status = Column(String)
    address = Column(String)
    lng = Column(Float)
    lat = Column(Float)
    summary = Column(String)
    description = Column(String)

class Cluster(Base):
    __tablename__ = 'clusters'
    id_=Column(Integer,primary_key=True)
    batch_id=Column(Integer)
    city_id=Column(Integer)
    request_type_id=Column(Integer)
    score=Column(Integer)
    lat=Column(Float)
    lng=Column(Float)

class ClusterIssue(Base):
    __tablename__ = 'clusters_issues'
    cluster_id=Column(Integer,primary_key=True)
    issue_id=Column(Integer)
    batch_id=Column(Integer)
    city_id=Column(Integer)

class Batch(Base):
    __tablename__ = 'batches'
    id_ = Column(Integer,primary_key=True)
    city_id = Column(Integer,primary_key=True)
    created_at = Column(String)

class RequestType(Base):
    __tablename__ = "request_types"
    id_ = Column(Integer,primary_key=True)
    city_id = Column(Integer)
    name = Column(String)

class City(Base):
    __tablename__ = 'cities'
    id_ = Column(Integer,primary_key=True)
    name = Column(String)
    lat = Column(Float)
    lng = Column(Float)

def direct_upload(city):
    ## connect to local database
    client = MongoClient()
    db = client.scf_data

    ## connect to remote database
    with open('database_info.json','r') as f:
        database_string = json.load(f)["database_string"]
    engine = create_engine(database_string,echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    print "uploading data..."
    key=json.load(open('key_file.json','r'))["key"]
    base_url = "http://floating-forest-13652.herokuapp.com/"

    #connect to mongodb server and create database...
    client = MongoClient()
    db= client.scf_data

    print "........uploading streets"
    ## upload streets, first get all issues into array
    streets_cursor = db.streets.find({"city_id":city["id_"]},no_cursor_timeout=True)
    streets = [street for street in streets_cursor]
    streets_cursor.close()

    existing_ids = [id[0] for id in session.query(Street.id_)]
    streets_to_upload = [Street(
        id_=street["id_"],
        name=street["name"],
        city_id=street["city_id"],
        length=street["length"])
        for street in streets
            if not street["id_"] in existing_ids]

    session.add_all(streets_to_upload)
    session.commit()

    print "........uploading issues"
    ## upload issues, first get all issues into array
    issues_cursor = db.issues.find({"city_id":city["id_"]},no_cursor_timeout=True)
    issues = [issue for issue in issues_cursor]
    issues_cursor.close()
    existing_ids = [id[0] for id in session.query(Issue.id_)]

    issues_to_create=[]
    issues_to_update=[]
    for issue in issues:
        if not "street_id" in issue.keys():
            street_id=0
        else:
            street_id=issue["street_id"]
        if not issue["id_"] in existing_ids:
            issues_to_create.append(Issue(
                id_=issue["id_"],
                city_id=issue["city_id"],
                request_type_id = issue["request_type_id"],
                street_id = street_id,
                created_at = issue["created_at"],
                status = issue["status"],
                address = issue["address"],
                lng = issue["lng"],
                lat = issue["lat"],
                summary = issue["summary"],
                description = issue["description"]))
        else:
            issues_to_update.append(Issue(
                id_=issue["id_"],
                city_id=issue["city_id"],
                request_type_id = issue["request_type_id"],
                street_id = issue["street_id"],
                created_at = issue["created_at"],
                status = issue["status"],
                address = issue["address"],
                lng = issue["lng"],
                lat = issue["lat"],
                summary = issue["summary"],
                description = issue["description"]))

    print ".........................updating old issues..."
    progress = 0.
    for issue_to_update in issues_to_update:
        # print existing_ids
        # print issue
        session.query(Issue).filter_by(id_=issue["id_"]).update(issue)
        progress+=1
        print "progress: "+str(progress/len(issues_to_update)),"\r",
    print ".........................updating new issues..."
    session.add_all(issues_to_create)
    session.commit()

    ## upload latest batch
    latest_batch = db.batches.find({"city_id":city["id_"]}).sort([("id_",pymongo.DESCENDING)])[0]

    print "........uploading clusters"
    ## upload clusters and clusters issues with latest batch id
    clusters_cursor = db.clusters.find({"batch_id":latest_batch["id_"]})
    clusters = [cluster for cluster in clusters_cursor]
    cluster_ids = [cluster["id_"] for cluster in clusters]
    clusters_cursor.close()

    clusters_to_upload = [Cluster(
        id_=cluster["id_"],
        batch_id=cluster["batch_id"],
        city_id=cluster["city_id"],
        request_type_id=cluster["request_type_id"],
        score=cluster["score"],
        lat=cluster["lat"],
        lng=cluster["lng"])
        for cluster in clusters]

    session.add_all(clusters_to_upload)
    session.commit()

    print "........uploading clusters issues"
    ## upload clusters and clusters issues with latest batch id
    clusters_issues_cursor = db.clusters_issues.find({"cluster_id":{"$in":cluster_ids}})
    clusters_issues = [cluster_issue for cluster_issue in clusters_issues_cursor]
    clusters_issues_cursor.close()

    clusters_issues_to_upload = [ClusterIssue(
        cluster_id=cluster_issue["cluster_id"],
        issue_id=cluster_issue["issue_id"],
        batch_id=latest_batch["id_"],
        city_id=city["id_"])
        for cluster_issue in clusters_issues]

    session.add_all(clusters_issues_to_upload)
    session.commit()

    print "........uploading batch and deleting old clusters"
    batch_to_upload = Batch(
        id_=latest_batch["id_"],
        city_id=latest_batch["city_id"],
        created_at=latest_batch["created_at"])
    session.add(batch_to_upload)
    session.commit()

    ## delete old batches of clusters
    session.query(Batch).filter(
        Batch.city_id==latest_batch["city_id"],
        Batch.id_ != latest_batch["id_"]).delete()
    session.query(Cluster).filter(
        Cluster.city_id==latest_batch["city_id"],
        Cluster.batch_id != latest_batch["id_"]).delete()
    session.query(ClusterIssue).filter(
        ClusterIssue.city_id==latest_batch["city_id"],
        ClusterIssue.batch_id != latest_batch["id_"]).delete()

    print "........uploading request types"
    ## upload request types
    request_types_to_upload = [RequestType(
        id_=request_type["id_"],
        city_id=request_type["city_id"],
        name=request_type["name"])
        for request_type in db.request_types.find({
            "city_id":city["id_"]})]
    session.add_all(request_types_to_upload)
    session.commit()

    print "........uploading city"
    ## upload cities
    city = db.cities.find_one({"id_":city["id_"]})
    city_to_upload = City(
        name=city["name"],
        id_=city["id_"],
        lat=city["lat"],
        lng=city["lng"])
    session.add(city_to_upload)
    session.commit()
