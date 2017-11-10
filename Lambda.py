import sys
import logging
import rds_config
import pymysql
import json
import datetime
#rds settings
rds_host  = "partyappdbv1.cc2vmnxwmq5r.us-east-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=3)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
def handler(event, context):
    """
    This function fetches content from mysql RDS instance
    """
    with conn.cursor() as cur:
        if (event['method'] == 'addNewUser'):
            cur.execute("CALL addNewUser(%s)", (event['uid'],))
            cur.execute("select * from users")
            conn.commit()
            r = [dict((cur.description[i][0], value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(value, datetime.datetime) else value) \
               for i, value in enumerate(row)) for row in cur]
            return r
            
        if (event['method'] == 'userAddsFriends'):
            for friendElem in event['friends']:
                cur.execute("CALL addFriend(%s, %s)", (event['uid'], friendElem))
            conn.commit()
            cur.execute("select * from friends")
            r = [dict((cur.description[i][0], value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(value, datetime.datetime) else value) \
               for i, value in enumerate(row)) for row in cur]
            return r
            
        if (event['method'] == 'partiesUserInvitedTo'):
            print("This requested all parties user has been invited to")
            return event
        
        if (event['method'] == 'userGetsPartiesHosting'):
            return event
        
        if (event['method'] == 'userCreatesNewParty'):
            cur.execute("CALL userCreatesNewParty(%s, %s, %s, %s, %s, %s, %s)", (event['uid'], event['partyAddress'], event['startTime'], event['endTime'], event['partyName'], event['partyType'], event['partyDescription']))
            cur.execute("select party_id,start_time,end_time,party_name,party_type,host_id from party")
            conn.commit()
            r = [dict((cur.description[i][0], value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(value, datetime.datetime) else value) \
               for i, value in enumerate(row)) for row in cur]
            return r
            
        if (event['method'] == 'userInvitesFriendsToParty'):
            for friendElem in event['friends']:
                friendId = friendElem['friendId']
                numInvites = friendElem['numInvites']
                cur.execute("CALL inviteUserToParty(%s, %s, %s, %s)", (friendId, event['partyid'], event['uid'], numInvites))
            conn.commit()
            cur.execute("select * from invitedTo")
            r = [dict((cur.description[i][0], value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(value, datetime.datetime) else value) \
               for i, value in enumerate(row)) for row in cur]
            return r
