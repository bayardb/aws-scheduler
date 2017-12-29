######################################################################################################################
#  Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://aws.amazon.com/asl/                                                                                    #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################


import boto3
import datetime
import json
from urllib2 import Request
from urllib2 import urlopen
from collections import Counter

def putCloudWatchMetric(region, instance_id, instance_state):

    cw = boto3.client('cloudwatch')

    cw.put_metric_data(
        Namespace='EC2Scheduler',
        MetricData=[{
            'MetricName': instance_id,
            'Value': instance_state,

            'Unit': 'Count',
            'Dimensions': [
                {
                    'Name': 'Region',
                    'Value': region
                }
            ]
        }]

    )

def addToStartOrStopLists(startList, stopList, tagValue, createMetrics, defaultStartTime, defaultStopTime, defaultTimeZone, defaultDaysActive, regionName, instanceId, instanceState, day, minTime, maxTime):
    ptag = tagValue.split(":")

    # Split out Tag & Set Variables to default
    startTime = defaultStartTime
    stopTime = defaultStopTime
    timeZone = defaultTimeZone
    daysActive = defaultDaysActive
    state = instanceState
    default1 = 'default'
    default2 = 'true'

    # Post current state of the instances
    if createMetrics == 'enabled':
        if state == "running":
            putCloudWatchMetric(regionName, instanceId, 1)
        if state == "stopped":
            putCloudWatchMetric(regionName, instanceId, 0)

    # Parse tag-value
    if len(ptag) >= 1:
        if ptag[0].lower() in (default1, default2):
            startTime = defaultStartTime
        else:
            startTime = ptag[0]
            stopTime = ptag[0]
    if len(ptag) >= 2:
        stopTime = ptag[1]
    if len(ptag) >= 3:
        timeZone = ptag[2].lower()
    if len(ptag) >= 4:
        daysActive = ptag[3].lower()

    isActiveDay = False

    # Days Interpreter
    if daysActive == "all":
        isActiveDay = True
    elif daysActive == "weekdays":
        weekdays = ['mon', 'tue', 'wed', 'thu', 'fri']
        if (day in weekdays):
            isActiveDay = True
    else:
        daysActive = daysActive.split("-")
        for d in daysActive:
            if d.lower() == day:
                isActiveDay = True
    # Append to start list
    if startTime >= str(minTime) and startTime <= str(maxTime) and \
        isActiveDay == True and state == "stopped":
        startList.append(instanceId)
        print instanceId, " added to START list"
        if createMetrics == 'enabled':
            putCloudWatchMetric(regionName, instanceId, 1)

    # Append to stop list
    if stopTime >= str(minTime) and stopTime <= str(maxTime) and \
        isActiveDay == True and state in ("running", "available"):
        stopList.append(instanceId)
        print instanceId, " added to STOP list"
        if createMetrics == 'enabled':
            putCloudWatchMetric(regionName, instanceId, 0)

def lambda_handler(event, context):

    print "Running MPayment AWS Resource Scheduler"

    ec2 = boto3.client('ec2')
    cf = boto3.client('cloudformation')
    outputs = {}
    stack_name = "MPaymentAWSResourceScheduler"
    response = cf.describe_stacks(StackName=stack_name)
    for e in response['Stacks'][0]['Outputs']:
        outputs[e['OutputKey']] = e['OutputValue']
    ddbTableName = outputs['DDBTableName']

    awsRegions = ec2.describe_regions()['Regions']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(ddbTableName)
    response = table.get_item(
        Key={
            'SolutionName': 'MPaymentAWSResourceScheduler'
        }
    )
    item = response['Item']



    # Reading Default Values from DynamoDB
    customTagName = str(item['CustomTagName'])
    customTagLen = len(customTagName)
    defaultStartTime = str(item['DefaultStartTime'])
    defaultStopTime = str(item['DefaultStopTime'])
    defaultTimeZone = 'utc'
    defaultDaysActive = str(item['DefaultDaysActive'])
    sendData = str(item['SendAnonymousData']).lower()
    createMetrics = str(item['CloudWatchMetrics']).lower()
    UUID = str(item['UUID'])
    TimeNow = datetime.datetime.utcnow().isoformat()
    TimeStamp = str(TimeNow)

    # Declare Dicts
    regionDict = {}
    allRegionDict = {}
    regionsLabelDict = {}
    postDict = {}

    for region in awsRegions:
        try:
            # Create connection to the EC2 using Boto3 resources interface
            ec2 = boto3.resource('ec2', region_name=region['RegionName'])
            rds = boto3.client('rds', region_name=region['RegionName'])

            awsregion = region['RegionName']
            now = datetime.datetime.now().strftime("%H%M")
            nowMax = datetime.datetime.now() - datetime.timedelta(minutes=59)
            nowMax = nowMax.strftime("%H%M")
            nowDay = datetime.datetime.today().strftime("%a").lower()

            # Declare Lists
            startList = []
            stopList = []
            rdsStartList = []
            rdsStopList = []

            # List all RDS instances
            rds_instances = rds.describe_db_instances()
            for rds_instance in rds_instances['DBInstances']:
                tagsList = rds.list_tags_for_resource(ResourceName=rds_instance['DBInstanceArn'])['TagList']
                if tagsList != None:
                    for t in tagsList:
                        if t['Key'][:customTagLen] == customTagName:
                            ptag = t['Value']
                            addToStartOrStopLists(rdsStartList, rdsStopList, ptag, createMetrics, defaultStartTime, defaultStopTime, defaultTimeZone, defaultDaysActive, region['RegionName'], rds_instance['DBInstanceIdentifier'], rds_instance['DBInstanceStatus'], nowDay, nowMax, now)
                else:
                    print "RDS Instance", rds_instance['DBInstanceIdentifier'], " tags not found."

            # List all instances
            instances = ec2.instances.all()
            print "Creating", region['RegionName'], "instance lists..."
            for i in instances:
                if i.tags != None:
                    for t in i.tags:
                        if t['Key'][:customTagLen] == customTagName:
                            ptag = t['Value']
                            addToStartOrStopLists(startList, stopList, ptag, createMetrics, defaultStartTime, defaultStopTime, defaultTimeZone, defaultDaysActive, region['RegionName'], i.instance_id, i.state['Name'], nowDay, nowMax, now)
            # Execute Start and Stop Commands
            if startList:
                print "Starting", len(startList), "instances", startList
                ec2.instances.filter(InstanceIds=startList).start()
            else:
                print "No Instances to Start"

            if stopList:
                print "Stopping", len(stopList) ,"instances", stopList
                ec2.instances.filter(InstanceIds=stopList).stop()
            else:
                print "No Instances to Stop"

            # Execute RDS Start
            if rdsStartList:
                print "RDS Starting", len(rdsStartList), "instances", rdsStartList
                for db in rdsStartList:
                    try:
                        response = rds.start_db_instance(DBInstanceIdentifier=db)
                        print("{0} status: {1}".format(db, response['DBInstanceStatus']))
                    except Exception as e:
                        print("Exception: {0}".format(e))
            else:
                print "No RDS Instances to Start"

            # Execute RDS Stop
            if rdsStopList:
                print "RDS Stoping", len(rdsStopList), "instances", rdsStopList
                for db in rdsStopList:
                    try:
                        response = rds.stop_db_instance(DBInstanceIdentifier=db)
                        print("{0} status: {1}".format(db, response['DBInstanceStatus']))
                    except Exception as e:
                        print("Exception: {0}".format(e))
            else:
                print "No RDS Instances to Stop"
        except Exception as e:
            print ("Exception: "+str(e))
            continue
