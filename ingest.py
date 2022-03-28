import pandas as pd
from neo4j import GraphDatabase
import json
from dotenv import load_dotenv
import boto3
import os


class ChainverseGraph:
    def __init__(self, uri, user, password):
        self.__driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


def write_df_to_s3(df, BUCKET, file_name, resource, s3, ACL="public-read"):
    df.to_csv(f"s3://{BUCKET}/{file_name}", index=False)
    object_acl = resource.ObjectAcl(BUCKET, file_name)
    response = object_acl.put(ACL=ACL)
    location = s3.get_bucket_location(Bucket=BUCKET)["LocationConstraint"]
    url = "https://s3-%s.amazonaws.com/%s/%s" % (location, BUCKET, file_name)
    return url


def read_df_from_s3(BUCKET, file_name):
    df = pd.read_csv(f"s3://{BUCKET}/{file_name}", lineterminator="\n")
    return df


def set_object_private(BUCKET, file_name, resource):
    object_acl = resource.ObjectAcl(BUCKET, file_name)
    response = object_acl.put(ACL="private")


def update_identifiers(url, conn):

    iden_rel_query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' as identifiers
                    MATCH (d:Dao:Daohaus:Entity {{address: identifiers.address}})
                    SET d.name = identifiers.name, d.twitter = identifiers.twitter, d.forum = identifiers.forum, d.discord = identifiers.discord, d.blog = identifiers.blog, 
                        d.documentation = identifiers.documentation, d.opensea = identifiers.opensea, d.gallery = identifiers.gallery, d.github = identifiers.github   
                    """
    conn.query(iden_rel_query)
    print("identifier information added")


if __name__ == "__main__":

    load_dotenv()
    uri = os.getenv("NEO_URI")
    username = os.getenv("NEO_USERNAME")
    password = os.getenv("NEO_PASSWORD")
    conn = ChainverseGraph(uri, username, password)

    resource = boto3.resource("s3")
    s3 = boto3.client("s3")
    BUCKET = "chainverse"

    df = pd.read_csv("daohaus_identifiers_late_feb.csv")
    print(df.columns)

    identifiers_dict = []
    raw_identifiers = df.to_dict(orient="records")

    for row in raw_identifiers:

        current_dict = {}
        if not row["contract"]:
            continue

        current_dict["address"] = row["contract"]

        if row["name"]:
            current_dict["name"] = row["name"]
        else:
            current_dict["name"] = ""

        try:
            x = row["twitter"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["twitter"] = item

        try:
            x = row["forum"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["forum"] = item

        try:
            x = row["discord"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["discord"] = item

        try:
            x = row["blog"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["blog"] = item

        try:
            x = row["documentation"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["documentation"] = item

        try:
            x = row["opensea"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["opensea"] = item

        try:
            x = row["gallery"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["gallery"] = item

        try:
            x = row["github"].replace("'", '"')
            items = json.loads(x)
            item = items[0]
        except:
            item = ""
        current_dict["github"] = item

        identifiers_dict.append(current_dict)

    identifier_df = pd.DataFrame(identifiers_dict)
    url = write_df_to_s3(identifier_df, BUCKET, "daohaus/identifiers/identifier.csv", resource, s3)
    update_identifiers(url, conn)

    print(identifiers_dict)
