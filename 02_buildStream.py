#!/usr/bin/python2.7 -S
# -*- coding: utf-8 -*-
# called inline by 01_buildStream.sh
# Author: Mark Teehan, March 2018.
import sys
from random import randrange
import random

from datetime import timedelta, datetime
import pyhdb
import os, shutil, fnmatch
from time import gmtime, strftime
from string import Template

vTableName=sys.argv[1]
vStringNumber=sys.argv[2]

conn = pyhdb.connect(host='xxx.xxx.xxx.corp',port=3xx15,user='xx', password='xx')

vDebug='N'

def filter_non_printable(str):
  return ''.join([c for c in str if ord(c) > 31 or ord(c) == 9 or ord(c)>26])

def timeStamped(fmt='%Y%m%d_%H%M%S'):
    return datetime.now().strftime(fmt)

def tsString(fmt='%Y%m%d%H%M%S'):
    return datetime.now().strftime(fmt)

def Debug(pMsg):
  if vDebug == 'Y':
    print("[" + "Debug " +pMsg + "]")



def runSQL(pSQL):
  # select and return a single value
  Debug("in RunSQL")

  if vFake == 'Y':
    Debug("Fake=N: executing " +pSQL)
  else:
    try:
      C1.execute(pSQL)
      for row in C1.fetchall():
        return row[0].encode('utf-8')
    except pyhdb.Error, e:
      print "pyHDB Error [%d]: %s" % (e.args[0], e.args[1])

def testConnection():
  print(runSQL("Select 'Test Connection: '|| count(*) from objects"))

def checkTable (pTableName):
  global vBytes
  fn=" checkTable".ljust(15,' ')
  vTS=tsString()
  #print timeStamped() + fn + ": (%s)" % (pTableName)
  vSQL = """SELECT count(*) cc from tables where schema_name='BDS_STREAMS' and lower(table_name)=lower('%s') """ % (pTableName)
  C1.execute(vSQL)
  for C1_row in C1.fetchall():
    vCount =C1_row[0]
  if (vCount <> 1):
    #print "Error: table name %s not found in the BDS_STREAMS schema. Check, and restart" % (pTableName)
    a=0
  else:
    vSQL = """SELECT count(*) cc from table_columns where schema_name='BDS_STREAMS' and lower(table_name)=lower('%s') """ % (pTableName)
    C1.execute(vSQL)
    for C1_row in C1.fetchall():
      vColCount =C1_row[0]
    if (vColCount==0 ):
      #print "Error: table name %s not found in the BDS_STREAMS schema. Check, and restart" % (pTableName)
      a=0
    else:
      #print "Table %s has %s columns." %(pTableName, str(vColCount))
      vSQL = """SELECT lower(column_name), position, data_type_name, length
                  from table_columns
                 where schema_name='BDS_STREAMS'
                   and lower(table_name)=lower('%s')
              order by position""" % (pTableName)
      C2.execute(vSQL)
      # Build Four strings:
      # a. vCreateSchema: CCL create schema command
      # b. SELECT statement for the stream
      # c. selectExpr is the streamed dataframe select for spark streams
      # d. [Experimental] a Solr.xml - a schema definition to stream and auto-index the data into Solr

      vCreateSchema="create schema " + pTableName + "_schema ( "
      vSelect="select "
      vSelectExpr="selectExpr("
      vSolrSchema=" "
      for C2_row in C2.fetchall():
        vColumn_name =C2_row[0]
        vPosition =C2_row[1]
        vData_Type_Name =C2_row[2]
        vLength =C2_row[3]
        if (vPosition==1):
           vComma=" "
        else:
           vComma=" ,"

        if (vPosition>=1):
          #a - vCreateSchema
          vCreateSchema = vCreateSchema + vComma + vColumn_name + " "
          #a - vSelect
          vSelect = vSelect + vComma + 'A.'+ vColumn_name + " "
          #c - selectExpr
          if (vData_Type_Name == 'VARCHAR' or vData_Type_Name == 'NVARCHAR'):
            vCreateSchema = vCreateSchema + 'string '
            vSelectExpr = vSelectExpr + ''' %s "split(value,',')[%s] as %s"''' % (vComma, str(vPosition-1),vColumn_name)
            vSolrSchema= vSolrSchema +  \
            ' <field name="%s" type="%s" indexed="true" stored="true" multiValued="false" required="true"/>' % (vColumn_name,vData_Type_Name) + "\n"
          else :
            vCreateSchema = vCreateSchema + ' String '
            vSelectExpr = vSelectExpr + ''' %s "cast(split(value,',')[%s] as int) as %s" ''' % (vComma, str(vPosition-1),vColumn_name)
            vSolrSchema= vSolrSchema +  \
            ' <field name="%s" type="%s" indexed="true" stored="true" multiValued="false" required="true"/>' % (vColumn_name, vData_Type_Name ) + "\n"

      if (vPosition == vColCount):
        vCreateSchema = vCreateSchema + '); '
        vSelect = vSelect + 'from stream_'+ pTableName+' A;'
        vSelectExpr=vSelectExpr+''',"split(value,',')[18] as gen_ts","current_timestamp as sps_ts","regexp_replace(substring(timestamp,1,13),' ','_')  as partition_key") '''
    # end table has > 1 column
        vSolrSchema=vSolrSchema+""


    if (vStringNumber=="1"):
      print vCreateSchema
    if (vStringNumber=="2"):
      print vSelect
    if (vStringNumber=="3"):
      print vSelectExpr
    if (vStringNumber=="4"):
      print vSolrSchema

# starts here

C1=conn.cursor()
C2=conn.cursor()
vTs=timeStamped()

testConnection()
checkTable(vTableName)
