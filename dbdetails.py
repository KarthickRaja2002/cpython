import os
import sys
import app.classes.utils

utils = app.classes.utils
from app.sql.db import DB


class DBQuery(object):
    def __init__(self, buildid, logger):
        self.buildid = buildid
        self.logger = logger
        self.mysql = DB(logger)

    def productname_btypename(self, id=False):
        build_log_details = self.mysql.query("select prod_id,b_type_id from BuildLog where build_id = '{buildid}';".format(buildid=self.buildid))

        if build_log_details:
            prodname = build_log_details[0]
            btypename = build_log_details[1]
            if id:
                return (prodname, btypename)

            if str(build_log_details[0]).isdigit() and str(build_log_details[1]).isdigit():
                return (self.getnamefromprodId(self.mysql, prodname), self.getnamefrombtypeId(self.mysql, btypename))

        return (None, None)

    def prodname(self, id=False):
        prodname = self.mysql.query("select prod_id from BuildLog where build_id = '{buildid}';".format(buildid=self.buildid))

        # -- prodname Id
        if prodname and id:
            return prodname[0]

        # -- prodname Str
        if prodname and str(prodname[0]).isdigit():
            return self.getnamefromprodId(self.mysql, prodname[0])
        else:
            return None

    def build_type_id(self):
        build_type_id = self.mysql.query("select b_type_id from BuildLog where build_id='" + str(self.buildid) + "';")
        if build_type_id:
            return build_type_id[0]

        return None

    def btypename(self, id=False):
        btypename = self.mysql.query("select b_type_id from BuildLog where build_id = '{buildid}';".format(buildid=self.buildid))

        # -- Btype Id
        if btypename and id:
            return btypename[0]

        # -- Btype Str
        if btypename and str(btypename[0]).isdigit():
            return self.getnamefrombtypeId(self.mysql, btypename[0])
        else:
            return None

    def get_build_url(self) -> str:
        build_url = self.mysql.query("select build_url from BuildLog where build_id = '{buildid}'".format(buildid=self.buildid))
        if build_url:
            return build_url[0]
        else:
            return None

    def get_url_id(self):
        urlId = self.mysql.query("select url_id from BuildLog where build_id = '{buildid}'".format(buildid=self.buildid))
        if urlId:
            return urlId[0]
        else:
            return None

    def get_milestone_url(self, urlId) -> str:
        build_url = self.mysql.query(f"select milestone_url from URLDetails where id = {urlId}")
        if build_url:
            return build_url[0]
        else:
            return None

    def zacprodname(self):
        zacprodname = self.mysql.query("select zac_prod_name from Products where prod_name = '{prodname}';".format(prodname=self.prodname()))
        if zacprodname:
            return zacprodname[0]
        else:
            return None

    def zacdetails(self):
        try:
            zac = dict()
            zac_details = self.mysql.query(
                "select zac_domain_name,zac_api_key,zac_username,zac_password,zac_ip from BuildType where b_type = '{btype}';".format(
                    btype=self.btypename()))
            zac = {'zac_domain_name': zac_details[0] if zac_details[0] else None,
                   'zac_api_key': zac_details[1] if zac_details[1] else None,
                   'zac_username': zac_details[2] if zac_details[2] else None,
                   'zac_password': zac_details[3] if zac_details[3] else None,
                   'zac_ip': zac_details[4] if zac_details[4] else None}
            return zac

        except Exception as err:
            self.logger.log.info(
                'Function : {0} has deducted an exception : {1} in line number : {2}'.format(utils.whoami(), err, sys.exc_info()[-1].tb_lineno))
            sys.exit(1)

    def ipsentry(self, ipskey=None):
        result = dict()
        ipsentry = self.mysql.queryall(
            "select search_string from IpsPropertyConfig where prod_id = '{prodid}' and b_type_id = '{btypeid}';".format(prodid=self.prodname(True),
                                                                                                                         btypeid=self.btypename(
                                                                                                                             True)))
        ipslist = self.listoftupletolist(ipsentry)
        if ipskey is not None:
            for ips in ipslist:
                if ipskey in ips:
                    result[ips] = None
            return result
        return ipslist

    def subprodof(self):
        subprodof = self.mysql.query("select sub_prod_of from Products where prod_name = '{prodname}';".format(prodname=self.prodname()))
        if subprodof and subprodof[0] != '':
            return subprodof[0]
        else:
            return self.prodname()

    @staticmethod
    def getnamefromprodId(mysql, prod_id):
        prodname = mysql.query("select prod_name from Products where id = '{prodid}';".format(prodid=prod_id))
        if prodname:
            return prodname[0]
        else:
            return None

    @staticmethod
    def getnamefrombtypeId(mysql, btype_id):
        btypename = mysql.query("select b_type from BuildType where id = '{btypeid}';".format(btypeid=btype_id))
        if btypename:
            return btypename[0]
        else:
            return None

    @staticmethod
    # -- query fetchone
    def tupletolist(tupledata):
        return list(tupledata)

    @staticmethod
    # -- query fetch all
    def listoftupletolist(tupledata):
        dictionary = dict()
        for tuple in tupledata:
            for items in tuple:
                dictionary[items] = None

        return list(dictionary.keys())

    def isSpecificTar(self, tarname, prod_id, btype_id):
        tar_file_details = self.mysql.query(
            f"select tar_filename from TarFileNames where b_type_id='{btype_id}' and prod_id='{prod_id}' and tar_filename= '{tarname}';")
        return (not tar_file_details)

    def upsertSpecificTarStatus(self, tar, status, log, diffreport, formatted_date):
        tableid = self.mysql.query("select id from SpecifictarCreation where build_id='" + str(self.buildid) + "' and tar_name='" + str(tar) + "';")
        self.logger.log.info(f"TABLE ID : {tableid}")
        if not tableid:
            sql = "INSERT into SpecifictarCreation (build_id,tar_name,status,log,diffreport,updated_at,created_at,updated_by) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}');".format(
                self.buildid, tar, status, log, diffreport, formatted_date, formatted_date, '14')
        else:
            sql = "UPDATE SpecifictarCreation set status='{0}', diffreport='{1}', updated_at='{2}' where id='{3}';".format(
                status, diffreport, formatted_date, tableid[0])
        self.mysql.insert(sql)

    def getTarId(self, tar, formatted_date):
        tarid = self.mysql.query("select id from TarNames where tarname='" + str(tar) + "';")
        if not tarid:
            inserttarid = "INSERT into TarNames (tarname,updated_at,created_at,updated_by) VALUES ('{0}','{1}','{2}','{3}');".format(tar,
                                                                                                                                     formatted_date,
                                                                                                                                     formatted_date,
                                                                                                                                     '14')
            tarid = self.mysql.insert(inserttarid)
        else:
            tarid = tarid[0]

        return tarid

    def getPbId(self, prodId, buildtypeId, formatted_date):
        pbid = self.mysql.query("select id from ProductBuildTypes where prod_id='" + str(prodId) + "' and b_type_id='" + str(buildtypeId) + "';")
        if not pbid:
            insertpbid = "INSERT into ProductBuildTypes (prod_id,b_type_id,updated_at,created_at,updated_by) VALUES ('{0}','{1}','{2}','{3}','{4}');".format(
                prodId, buildtypeId, formatted_date, formatted_date, '14')
            pbid = self.mysql.insert(insertpbid)
        else:
            pbid = pbid[0]
        return pbid

    def getPbTarId(self, pbid, tarid, formatted_date):
        # -- fetch id from ProductBuildSpecificTarMapping Table
        pbtar_id = self.mysql.query("select id from ProductBuildSpecificTarMapping where pb_id='" + str(pbid) + "' and tar_id='" + str(tarid) + "';")
        if not pbtar_id:
            insertpbtar_id = "INSERT into ProductBuildSpecificTarMapping (pb_id,tar_id,updated_at,created_at,updated_by) VALUES ('{0}','{1}','{2}','{3}','{4}');".format(
                pbid, tarid, formatted_date, formatted_date, '14')
            pbtar_id = self.mysql.insert(insertpbtar_id)
        else:
            pbtar_id = pbtar_id[0]

        return pbtar_id

    def insertOrUpdateChecksum(self, tar, prodId, tarchecksum, formatted_date):
        buildtypeId = self.build_type_id()
        tarid = self.getTarId(tar, formatted_date)
        pbid = self.getPbId(prodId, buildtypeId, formatted_date)
        pbtar_id = self.getPbTarId(pbid, tarid, formatted_date)
        checksumid = self.mysql.query("select id from TarChecksums where build_id='" + str(self.buildid) + "' and pbtar_id='" + str(pbtar_id) + "';")
        if checksumid:
            sql = "UPDATE TarChecksums set md5sum='{0}',updated_at='{2}' where id='{1}';".format(tarchecksum, checksumid[0], formatted_date)
        else:
            sql = "INSERT into TarChecksums (build_id,pbtar_id,md5sum,updated_at,created_at,updated_by) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}');".format(
                self.buildid, pbtar_id, tarchecksum, formatted_date, formatted_date, '14')

        self.mysql.insert(sql)
        self.logger.log.info(
            'pbid - {0} && tarid - {1} && pbtarid - {2} && checksum id - {3} && checksum val - {4}'.format(pbid, tarid, pbtar_id, checksumid,
                                                                                                           tarchecksum))
