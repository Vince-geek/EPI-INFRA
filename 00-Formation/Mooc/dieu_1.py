#!/bin/bash
# -*- coding: utf-8 -*-

# ------------------------------------------------------------------------------------------------
# NAME		    :	stats_enedis.py
# AUTHOR	    :   MATTHIEU PICARD
# DATE		    :	23/11/2017
# PARAMETERS	:	NONE
# VERSION	    :	1.00
# ------------------------------------------------------------------------------------------------

import glob
import os
import re
import sys

sys.path.append('/mnt/data/rundeck/lib/')
import common_python_lib

from pgdb import connect

common_python_lib.JOB_NAME = "@job.name@"
common_python_lib.JOB_DESCRIPTION = "ENEDIS - Monthly statistics"
common_python_lib.LOG_LEVEL = "@job.loglevel@"
common_python_lib.VERSION = "V1.0"

cpt_bases = 0
nb_bdd_empty = 0
nb_bdd_empty_30d = 0

csv_rows_bases = []
csv_rows_users = []
csv_colname_bases = ['Server', 'Base', 'Size', 'Number of records stored in the last 30 days',
                     'Number of records viewed in the last 30 days']
csv_colname_users = ['Server', 'Base', 'Login', 'Group', 'Nb']

common_python_lib.fi_getipaddress()
common_python_lib.fi_setlogfile()
common_python_lib.fi_setheader()

csv_file_bases = "/mnt/data/rundeck/" + (common_python_lib.RDK_LOG_FILE.replace("_", "_Bases_", 1)).replace(".log",
                                                                                                            ".csv")
csv_file_users = "/mnt/data/rundeck/" + (common_python_lib.RDK_LOG_FILE.replace("_", "_Users_", 1)).replace(".log",
                                                                                                            ".csv")

csv_rows_bases.append(csv_colname_bases)
csv_rows_users.append(csv_colname_users)

for filename in glob.iglob('/maille/*/docubase/base/*/MDMNG.INF'):
    od_size = 0
    maille_folder = filename.replace("/MDMNG.INF", "/")
    maille_name = maille_folder.split('/')
    for item in os.listdir(maille_folder):
        itempath = os.path.join(maille_folder, item)
        od_size += os.path.getsize(itempath)

    for line in open(filename):
        line = line.rstrip()
        if re.search('^ID = [0-9]+$', line):
            colname, coltemp, od_now = line.split(' ')

        if re.search('^ID = [0-9]+ [/].*.[Oo][Dd]$', line):
            colname, coltemp, od_id, od_folder = line.split(' ')
            if int(od_id) <= int(od_now):
                if re.match(r'^/archive', od_folder):
                    od_size += os.path.getsize(od_folder)

    common_python_lib.fi_converthumansize(od_size)
    common_python_lib.fi_log("INFO", "Database name: " + maille_name[5] + ", Size: " + common_python_lib.retSize)
    csv_rows_bases.append([common_python_lib.SERVERNAME, maille_name[5], common_python_lib.retSize])

common_python_lib.fi_log("INFO", "**")

try:
    common_python_lib.fi_log("INFO", "PostgreSQL - Database: RECMPRD")
    db = connect(host="127.0.0.1:5432", user="docubase", password="docubase", database="RECMPRD")
    common_python_lib.fi_post_cmd("db = connect(host=127.0.0.1:5432,user=docubase,password=docubase,database=RECMPRD)")

    common_python_lib.fi_log("INFO", "PostgreSQL - Database: AMFPRD")
    db2 = connect(host="127.0.0.1:5432", user="docubase", password="docubase", database="AMFPRD")
    common_python_lib.fi_post_cmd("db2 = connect(host=127.0.0.1:5432,user=docubase,password=docubase,database=AMFPRD)")

    common_python_lib.fi_log("INFO", "PostgreSQL - Database connecting in progres...")
except Exception:
    common_python_lib.fi_log("ERROR", "Unable to connect to PostgreSQL database")
else:
    common_python_lib.fi_log("INFO", "PostgreSQL - Databases connect: OK")
    cur = db.cursor()
    cur2 = db2.cursor()

    cur.execute('SHOW server_version')
    common_python_lib.fi_post_cmd("cur.execute(SHOW server_version)")
    db_version = cur.fetchone()
    common_python_lib.fi_log("INFO", "PostgreSQL - Database version: " + db_version[0])

    cur.execute("SELECT pg_size_pretty(pg_database_size('RECMPRD'))")
    common_python_lib.fi_post_cmd("SELECT pg_size_pretty(pg_database_size(RECMPRD))")
    db_size = cur.fetchone()
    common_python_lib.fi_log("INFO", "PostgreSQL - RECMPRD Database size: " + db_size[0])

    cur.execute("SELECT pg_size_pretty(pg_database_size('AMFPRD'))")
    common_python_lib.fi_post_cmd("SELECT pg_size_pretty(pg_database_size(AMFPRD))")
    db_size2 = cur.fetchone()
    common_python_lib.fi_log("INFO", "PostgreSQL - AMFPRD Database size: " + db_size2[0])

    common_python_lib.fi_log("INFO", "Query in progress...")

    try:
        cur.execute(
            """SELECT keyrm_context,contextname FROM rm_context WHERE CAST(contextname AS text) ~* '.*\.[0-9]'""")
        common_python_lib.fi_post_cmd(
            "SELECT keyrm_context,contextname FROM rm_context WHERE CAST(contextname AS text) ~* .*\.[0-9]")
        bdd_ged = cur.fetchall()
    except Exception:
        common_python_lib.fi_log("ERROR", "Error during the request: Database GED count")
    else:
        common_python_lib.fi_log("INFO", "Database GED count: " + str(len(bdd_ged)))

    for id in bdd_ged:
        cpt_bases += 1
        common_python_lib.fi_log("INFO", "**")
        # common_python_lib.fi_log("INFO", "Database name: " + str(id[1]) + ", ID: " + str(id[0]))

        # try:
        #    cur.execute("""SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE 'net.docubase.events.store' AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND rm_event.keyrm_context = %s""", (id[0],))
        #    common_python_lib.fi_post_cmd("SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE net.docubase.events.store AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND rm_event.keyrm_context = %s")
        #    nb_store = cur.fetchone()
        # except Exception:
        #    common_python_lib.fi_log("ERROR", "Error during request: Number of records stored")
        # else:
        #    common_python_lib.fi_log("INFO", "Number of records stored: " + str(nb_store[0]))
        #    if nb_store[0] == 0:
        #        nb_bdd_empty += 1

        maille_name, maille_domain = id[1].split('.')
        maille_id = id[0]
        common_python_lib.fi_log("INFO",
                                 "Database [" + str(cpt_bases) + "/" + str(len(bdd_ged)) + "], Name: " + maille_name)

        try:
            cur.execute(
                """SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE 'net.docubase.events.store' AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval '1 mon')) AND rm_event.keyrm_context = %s""",
                (id[0],))
            common_python_lib.fi_post_cmd(
                "SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE net.docubase.events.store AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval 1 mon)) AND rm_event.keyrm_context = %s")
            nb_store_30d = cur.fetchone()
        except Exception:
            common_python_lib.fi_log("ERROR", "Error during request: Number of records stored in the last 30 days")
        else:
            common_python_lib.fi_log("INFO", "Number of records stored in the last 30 days: " + str(nb_store_30d[0]))
            if nb_store_30d[0] == 0:
                nb_bdd_empty_30d += 1

        # try:
        #    cur.execute("""SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE 'net.docubase.events.display' AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND rm_event.keyrm_context = %s""", (id[0],))
        #    common_python_lib.fi_post_cmd("cur.execute(SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE net.docubase.events.display AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND rm_event.keyrm_context = %s")
        #    nb_doc_read = cur.fetchone()
        # except Exception:
        #    common_python_lib.fi_log("ERROR", "Error during the request: Number of records viewed")
        # else:
        #    common_python_lib.fi_log("INFO", "Number of records viewed: " + str(nb_doc_read[0]))

        try:
            cur.execute(
                """SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE 'net.docubase.events.display' AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval '1 mon')) AND rm_event.keyrm_context = %s""",
                (id[0],))
            common_python_lib.fi_post_cmd(
                "cur.execute(SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE net.docubase.events.display AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval 1 mon))) AND rm_event.keyrm_context = %s")
            nb_doc_read = cur.fetchone()
        except Exception:
            common_python_lib.fi_log("ERROR", "Error during the request: Number of records viewed in the last 30 days")
        else:
            common_python_lib.fi_log("INFO", "Number of records viewed in the last 30 days: " + str(nb_doc_read[0]))

        for (i, item) in enumerate(csv_rows_bases):
            if maille_name in item:
                csv_rows_bases[i].append(str(nb_store_30d[0]))
                csv_rows_bases[i].append(str(nb_doc_read[0]))

        if int(nb_doc_read[0]) > 0:
            try:
                cur.execute(
                    """SELECT DISTINCT rm_actor.keyrm_actor, actorlogin, COUNT(rm_event.keyrm_eventtype) FROM rm_event, rm_actor, rm_eventtype WHERE rm_actor.keyrm_actor = rm_event.keyrm_actor AND rm_eventtype.eventtypename LIKE 'net.docubase.events.display' AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval '1 mon')) AND rm_event.keyrm_context = %s GROUP BY rm_actor.keyrm_actor""",
                    (id[0],))
                common_python_lib.fi_post_cmd(
                    "cur.execute(SELECT DISTINCT rm_actor.keyrm_actor, actorlogin, COUNT(rm_event.keyrm_eventtype) FROM rm_event, rm_actor, rm_eventtype WHERE rm_actor.keyrm_actor = rm_event.keyrm_actor AND rm_eventtype.eventtypename LIKE net.docubase.events.display AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval 1 mon)) AND rm_event.keyrm_context = %s GROUP BY rm_actor.keyrm_actor")
                list_logon = cur.fetchall()
            except Exception:
                common_python_lib.fi_log("ERROR", "Error during request: List of connections in the last 30 days")
            else:
                common_python_lib.fi_log("INFO", "List of connections in the last 30 day:")
                for logon in list_logon:
                    try:
                        cur2.execute(
                            """SELECT users.user_longname, groups.group_name FROM users, link_users_groups, groups WHERE user_name = %s AND users.keyusers = link_users_groups.keyusers AND link_users_groups.keygroups = groups.keygroups""",
                            (logon[1],))
                        common_python_lib.fi_post_cmd(
                            "cur2.execute(SELECT users.user_longname, groups.group_name FROM users, link_users_groups, groups WHERE user_name = %s AND users.keyusers = link_users_groups.keyusers AND link_users_groups.keygroups = groups.keygroups, (logon[1],))")
                        logon_infos = cur2.fetchall()
                    except Exception:
                        common_python_lib.fi_log("ERROR", "Error during request: Logon info")
                    else:
                        for logon_info in logon_infos:
                            common_python_lib.fi_log("INFO", "Login: " + str(logon[1]) + ", Username: " + str(
                                logon_info[0]) + ", Group: " + str(logon_info[1]) + ", Nb: " + str(logon[2]))
                            csv_rows_users.append(
                                [common_python_lib.SERVERNAME, maille_name, logon[1], logon_info[1], logon[2]])
        else:
            csv_rows_users.append([common_python_lib.SERVERNAME, maille_name, "", "", "0"])

    common_python_lib.fi_csv(csv_file_bases, csv_rows_bases)
    common_python_lib.fi_csv(csv_file_users, csv_rows_users)

    common_python_lib.fi_log("INFO", "**")
    # common_python_lib.fi_log("INFO", "Number of databases without records: " + str(nb_bdd_empty) + "/" + str(len(bdd_ged)))
    common_python_lib.fi_log("INFO", "Number of databases without records in the last 30 days: " + str(
        nb_bdd_empty_30d) + "/" + str(len(bdd_ged)))

    try:
        cur.execute(
            """SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE 'net.docubase.events.display' AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval '1 mon'))""")
        common_python_lib.fi_post_cmd(
            "cur.execute(SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE net.docubase.events.display AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval 1 mon)))")
        nb_doc_read = cur.fetchone()
    except Exception:
        common_python_lib.fi_log("ERROR",
                                 "Error during the request: Number of records viewed in the last 30 days for all databases")
    else:
        common_python_lib.fi_log("INFO", "Number of records viewed in the last 30 days for all databases: " + str(
            nb_doc_read[0]))

    try:
        cur.execute(
            """SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE 'net.docubase.events.store' AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval '1 mon'))""")
        common_python_lib.fi_post_cmd(
            "cur.execute(SELECT count(*) FROM rm_event, rm_eventtype WHERE rm_eventtype.eventtypename LIKE net.docubase.events.store AND rm_event.keyrm_eventtype = rm_eventtype.keyrm_eventtype AND event_timestamp > (current_date - (interval 1 mon)))")
        nb_doc_store = cur.fetchone()
    except Exception:
        common_python_lib.fi_log("ERROR",
                                 "Error during the request: Number of records stored in the last 30 days for all databases")
    else:
        common_python_lib.fi_log("INFO", "Number of records stored in the last 30 days for all databases: " + str(
            nb_doc_store[0]))

    try:
        cur2.execute("""SELECT count(*) FROM users WHERE valid='1'""")
        common_python_lib.fi_post_cmd("cur2.execute(SELECT count(*) FROM users WHERE valid=1)")
        nb_users = cur2.fetchone()
    except Exception:
        common_python_lib.fi_log("ERROR", "Error during the request: Number of active user accounts")
    else:
        common_python_lib.fi_log("INFO", "Number of active user accounts: " + str(nb_users[0]))

    db.close
    common_python_lib.fi_post_cmd("db.close()")
    db2.close()
    common_python_lib.fi_post_cmd("db2.close()")
    common_python_lib.fi_log("INFO", "PostgreSQL - Database closed")
    common_python_lib.fi_log("INFO", "Bases files: " + csv_file_bases)
    common_python_lib.fi_log("INFO", "Users files: " + csv_file_users)

common_python_lib.fi_setfooter()