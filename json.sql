/*
json: objects and arrays
object {"name1":value1, "name2":"value2", ... }  The name is always in double quotes. strings are in double quotes
array  [multiple values, , , ..]
*/

create or replace directory polar as 'C:\Work\garmin_bu\Polar';
select * from table(get_file_name ('C:\Work\garmin_bu\Polar', 'json'));
select file_name, substr(file_name, instr (file_name, chr(92), 1, 4) +1) fn  from table (get_file_name ('C:\Work\garmin_bu\Polar', 'json')) order by file_name
fetch first 20 rows only;

create table polar_data (file_name  varchar2 (100) not null, polar_json clob not null, constraint check_jason check (polar_json is json));

declare
    l_polar_file bfile;
    l_polar_data clob;
  l_dest_offset   integer := 1;
  l_src_offset    integer := 1;
  l_bfile_csid    number  := 0;
  l_lang_context  integer := 0;
  l_warning       integer := 0;
begin
for f in (select file_name, substr(file_name, instr (file_name, chr(92), 1, 4) +1) fn  from table (get_file_name ('C:\Work\garmin_bu\Polar', 'json'))  where file_name like '%training-session%')
loop
  dbms_output.put_line (f.fn);
  l_dest_offset    := 1;
  l_src_offset     := 1;
  l_bfile_csid      := 0;
  l_lang_context   := 0;
  l_warning        := 0;
  l_polar_file := bfilename('POLAR', f.fn);
  dbms_lob.open (l_polar_file, dbms_lob.file_readonly);
  dbms_lob.createtemporary(l_polar_data, true);
  dbms_lob.loadclobfromfile (dest_lob => l_polar_data, src_bfile => l_polar_file , amount  => DBMS_LOB.lobmaxsize,
      dest_offset   => l_dest_offset, src_offset    => l_src_offset, bfile_csid    => l_bfile_csid , lang_context  => l_lang_context, warning       => l_warning);
  insert into polar_data values (f.file_name, l_polar_data);
--  dbms_output.put_line (dbms_lob.substr(l_polar_data, least(400, dbms_lob.getlength(l_polar_file)), 1));
  commit;
  dbms_lob.close (l_polar_file);
  dbms_lob.freetemporary (l_polar_data);
end loop;
end;
/







declare
    l_polar_file bfile;
    l_polar_data clob;
  l_dest_offset   integer := 1;
  l_src_offset    integer := 1;
  l_bfile_csid    number  := 0;
  l_lang_context  integer := 0;
  l_warning       integer := 0;
begin
for f in (select file_name, substr(file_name, instr (file_name, chr(92), 1, 4) +1) fn  from table (get_file_name ('C:\Work\garmin_bu\Polar', 'json')) order by file_name
fetch first 50 rows only)
loop
   file_to_clob (l_polar_data, 'POLAR', f.fn);
--  insert into polar_data values (f.file_name, l_polar_data);
  dbms_output.put_line (dbms_lob.substr(l_polar_data, least(400, dbms_lob.getlength(l_polar_file)), 1));
  commit;
  dbms_lob.close (l_polar_file);
-- dbms_lob.freetemporary (l_polar_data);
end loop;
end;
/



SELECT DBMS_JSON.get_index_dataguide(
         'POLAR_DATA',
         'POLAR_JSON',
         dbms_json.format_hierarchical,
         dbms_json.geojson+dbms_json.pretty) AS dg
FROM   polar_data;


select json_dataguide(polar_json,
         dbms_json.format_hierarchical,
         dbms_json.geojson+dbms_json.pretty) dg_doc from  polar_data;

with dg_t as (select json_dataguide(polar_json) dg_doc from  polar_data)
select jt.* from   dg_t,
       json_table(dg_doc, '$[*]'
         columns
           jpath   varchar2(40) path '$."o:path"',
           type    varchar2(10) path '$."type"',
           tlength number       path '$."o:length"') jt
order by jt.jpath;

exec dbms_json.create_view('V_POLAR_JSON', 'polar_data', 'polar_json', dbms_json.get_index_dataguide('polar_data', 'polar_json','$.'));
 select json_dataguide (polar_json, dbms_json.format_hierarchical, dbms_json.geojson+dbms_json.pretty) from polar_data

json_dataguide(polar_json,
         dbms_json.format_hierarchical,
         dbms_json.geojson+dbms_json.pretty) 

https://oracle-base.com/articles/12c/json-data-guide-12cr2#create-view
select 'exec  DBMS_JSON.rename_column(''polar_data'', ''polar_json'', ''' || jpath || ''', DBMS_JSON.' ||
case type when 'string' then 'TYPE_STRING' when 'number' then 'TYPE_NUMBER' when 'array' then 'TYPE_ARRAY' else type end || ', '''
||
rtrim(ltrim(replace(upper(reverse(substr(reverse (jpath),1, instr (reverse (jpath), '.') - 1))), ', ', '_'), '"'), '"') || ''');' cname
from (
with dg_t as (select json_dataguide(polar_json) dg_doc from  polar_data)
select jt.* from   dg_t,
       json_table(dg_doc, '$[*]'
         columns
           jpath   varchar2(40) path '$."o:path"',
           type    varchar2(10) path '$."type"',
           tlength number       path '$."o:length"') jt
           order by 1;
		   
		   

begin 
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.city', DBMS_JSON.TYPE_STRING, 'CITY');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.countryCode', DBMS_JSON.TYPE_STRING, 'COUNTRYCODE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.date', DBMS_JSON.TYPE_STRING, 'RUN_DATE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays', DBMS_JSON.TYPE_ARRAY, 'DEVICEDAYS');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.date.day', DBMS_JSON.TYPE_NUMBER, 'DAY');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.date.month', DBMS_JSON.TYPE_NUMBER, 'MONTH');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.date.year', DBMS_JSON.TYPE_NUMBER, 'YEAR');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.deviceId', DBMS_JSON.TYPE_STRING, 'DEVICEID');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.samples', DBMS_JSON.TYPE_ARRAY, 'SAMPLES');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.samples.heartRate', DBMS_JSON.TYPE_NUMBER, 'HEARTRATE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.samples.secondsFromDayStart', DBMS_JSON.TYPE_NUMBER, 'SECONDSFROMDAYSTART');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.samples.source', DBMS_JSON.TYPE_STRING, 'SOURCE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.deviceDays.userId', DBMS_JSON.TYPE_NUMBER, 'USERID');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.exportVersion', DBMS_JSON.TYPE_STRING, 'EXPORTVERSION');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.favouriteSports', DBMS_JSON.TYPE_ARRAY, 'FAVOURITESPORTS');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.favouriteSports[*]', DBMS_JSON.TYPE_STRING, 'FAVOURITESPORTS[*]');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.firstName', DBMS_JSON.TYPE_STRING, 'FIRSTNAME');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.lastName', DBMS_JSON.TYPE_STRING, 'LASTNAME');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.linkedApplications', DBMS_JSON.TYPE_ARRAY, 'LINKEDAPPLICATIONS');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.linkedOrganisations', DBMS_JSON.TYPE_ARRAY, 'LINKEDORGANISATIONS');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.motto', DBMS_JSON.TYPE_STRING, 'MOTTO');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.nickname', DBMS_JSON.TYPE_STRING, 'NICKNAME');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.phone', DBMS_JSON.TYPE_STRING, 'PHONE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation."height, cm"', DBMS_JSON.TYPE_NUMBER, 'HEIGHT_CM');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation."weight, kg"', DBMS_JSON.TYPE_NUMBER, 'WEIGHT_KG');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation.birthday', DBMS_JSON.TYPE_STRING, 'BIRTHDAY');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation.maximumHeartRate', DBMS_JSON.TYPE_NUMBER, 'MAXIMUMHEARTRATE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation.restingHeartRate', DBMS_JSON.TYPE_NUMBER, 'RESTINGHEARTRATE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation.sex', DBMS_JSON.TYPE_STRING, 'SEX');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation.sleepGoal', DBMS_JSON.TYPE_STRING, 'SLEEPGOAL');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation.trainingBackground', DBMS_JSON.TYPE_STRING, 'TRAININGBACKGROUND');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.physicalInformation.typicalDay', DBMS_JSON.TYPE_STRING, 'TYPICALDAY');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.samples.metSources', DBMS_JSON.TYPE_ARRAY, 'METSOURCES');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.samples.metSources[*]', DBMS_JSON.TYPE_STRING, 'METSOURCES[*]');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.samples.mets', DBMS_JSON.TYPE_ARRAY, 'METS');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.samples.mets.value', DBMS_JSON.TYPE_NUMBER, 'VALUE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.samples.steps', DBMS_JSON.TYPE_ARRAY, 'STEPS');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.samples.steps.value', DBMS_JSON.TYPE_NUMBER, 'VALUE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.activitySummaryCommented', DBMS_JSON.TYPE_STRING, 'ACTIVITYSUMMARYCOMMENTED');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.activitySummaryLiked', DBMS_JSON.TYPE_STRING, 'ACTIVITYSUMMARYLIKED');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.activityVisibility', DBMS_JSON.TYPE_STRING, 'ACTIVITYVISIBILITY');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.autoApproveFollowRequest', DBMS_JSON.TYPE_STRING, 'AUTOAPPROVEFOLLOWREQUEST');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.clubCommunication', DBMS_JSON.TYPE_STRING, 'CLUBCOMMUNICATION');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.dateFormat', DBMS_JSON.TYPE_STRING, 'DATEFORMAT');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.dateSeparator', DBMS_JSON.TYPE_STRING, 'DATESEPARATOR');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.firstDayOfWeek', DBMS_JSON.TYPE_STRING, 'FIRSTDAYOFWEEK');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.guidanceOrArticles', DBMS_JSON.TYPE_STRING, 'GUIDANCEORARTICLES');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.language', DBMS_JSON.TYPE_STRING, 'LANGUAGE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.measurementUnit', DBMS_JSON.TYPE_STRING, 'MEASUREMENTUNIT');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.myCommentCommented', DBMS_JSON.TYPE_STRING, 'MYCOMMENTCOMMENTED');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.newComment', DBMS_JSON.TYPE_STRING, 'NEWCOMMENT');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.newFollower', DBMS_JSON.TYPE_STRING, 'NEWFOLLOWER');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.newsletter', DBMS_JSON.TYPE_STRING, 'NEWSLETTER');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.profileVisibility', DBMS_JSON.TYPE_STRING, 'PROFILEVISIBILITY');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.receivedInvitation', DBMS_JSON.TYPE_STRING, 'RECEIVEDINVITATION');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.sessionLiked', DBMS_JSON.TYPE_STRING, 'SESSIONLIKED');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.timeFormat', DBMS_JSON.TYPE_STRING, 'TIMEFORMAT');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.timeFormatSeparator', DBMS_JSON.TYPE_STRING, 'TIMEFORMATSEPARATOR');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.timeZone', DBMS_JSON.TYPE_STRING, 'TIMEZONE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.settings.trainingSessionVisibility', DBMS_JSON.TYPE_STRING, 'TRAININGSESSIONVISIBILITY');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.state', DBMS_JSON.TYPE_STRING, 'STATE');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.street1', DBMS_JSON.TYPE_STRING, 'STREET1');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.username', DBMS_JSON.TYPE_STRING, 'USERNAME');
DBMS_JSON.rename_column('polar_data', 'polar_json', '$.zip', DBMS_JSON.TYPE_STRING, 'ZIP');
end;
/



declare 
l_table  varchar2 (30)   := 'polar_data';
l_column varchar2 (30)   := 'polar_json';
l_view     varchar2 (30) := 'V_POLAR';
l_dg clob;
begin
select json_dataguide (polar_json, dbms_json.format_hierarchical, dbms_json.geojson+dbms_json.pretty) into l_dg from polar_data;	 		 
  dbms_json.create_view(viewname  => l_view, tablename => l_table, jcolname  => l_column, dataguide =>  l_dg);
end;
/


BEGIN 
  dbms_json.create_view('V_POLAR_JSON', 'POLAR_DATA', 'POLAR_JSON', json_dataguide(polar_json, dbms_json.FORMAT_HIERARCHICAL, dbms_json.pretty), resolveNameConflicts => true);
END;
/



create table polar_data (file_name  varchar2 (100) not null, polar_json clob not null, constraint check_jason check (polar_json is json));
CREATE SEARCH INDEX polar_data_idx ON polar_data (polar_json) FOR JSON;
 
exec dbms_json.create_view_on_path ( 'v_polar', 'polar_data', 'polar_json', '$' );


select jt.* from polar_data pj, json_table (pj.polar_json, '$' columns (
  exportversion path '$.exportVersion'
, name  path '$.name'
, deviceid   path '$.deviceId'
, lattitude  path '$.latitude'
, longitude  path '$.longitude'
, starttime  path '$.startTime'
, stoptime  path '$.stopTime'
, timesoneoffset path '$.timeZoneOffset'
, distance path '$.distance'
, duration  path '$.duration'
, maximumheartrate  path '$.maximumHeartRate'
, averageheartrate  path '$.averageHeartRate'
, kilocalories   path '$.kiloCalories'
, dateTime path '$.physicalInformationSnapshot.dateTime'
, sexe  path '$.physicalInformationSnapshot.sex'
, birthday  path '$.physicalInformationSnapshot.birthday'
, vo2max path '$.physicalInformationSnapshot.vo2Max'
, tr_max_heartrate path '$.physicalInformationSnapshot.maximumHeartRate'
, restingheartrate path '$.physicalInformationSnapshot.restingHeartRate'
, aerobicthreshold path '$.physicalInformationSnapshot.aerobicThreshold'
, anaerobicthreshold  path '$.physicalInformationSnapshot.anaerobicThreshold'
, sleepgoal  path '$.physicalInformationSnapshot.sleepGoal'
, begintime  path '$.exercises.startTime'
, endtime path '$.exercises.stopTime'
, tzoffset path '$.exercises.timezoneOffset'
, eduration path '$.exercises.duration'
, edistance path '$.exercises.distance'
, sport  path '$.exercises.sport'
, elattitude  path '$.exercises.latitude'
, elongitude path '$.exercises.longitude'
, eascent path '$.exercises.ascent'
, edecent  path '$.exercises.descent'
, ekilocalories path '$.exercises.kiloCalories'
, min_hr  path '$.exercises.heartRate.min'
, avg_hr path '$.exercises.heartRate.avg'
, max_hr path '$.exercises.heartRate.max'
, avg_speed path '$.exercises.speed.avg'
, max_speed  path '$.exercises.speed.max'
, avg_cadence path '$.exercises.cadence.avg'
, max_cadence path '$.exercises.cadence.max'
, nested path '$.exercises.zones.heart_rate[*]'
                columns (
				                   linenum1 for ordinality,
                                                            lowerlimit path '$.lowerlimit',
                                                            inZone path '$.inZone',
                                                            zoneIndex path '$.zoneIndex')
, nested path '$.exercises.zones.speed[*]'                                                                                                                   
                columns (
                   linenum2 for ordinality,
                                                            slowerlimit path '$.lowerlimit',
                                                            shigerlimit path '$.higherLimit',
                                                            sinZone     path '$.inZone',
                                                            sdistance   path '$.distance',
                                                            szoneindex  path '$.zoneIndex')		
, nested path '$.exercises.zones.samples.altitude[*]'    	
                columns (
                   linenum3 for ordinality,
				   adatetime  path '$.dateTime',
				   avalue     path '$.value')				   
, nested path '$.exercises.zones.samples.recordedRoute[*]'    	
                columns (
                   linenum4 for ordinality,
				   rrdateTime     path '$.dateTime',
				   rrlongitude  path '$.longitude',
				   rrlatitude     path '$.latitude',
				   rraltitude     path '$.latitude')
, cardioload path '$.exercises.loadInformation.cardioLoad'			   
, muscleload path '$.exercises.loadInformation.muscleLoad'
, cardiointerpretation path '$.exercises.loadInformation.cardioLoadInterpretation'
, muscleloadinterpretation  path '$.exercises.loadInformation.muscleLoadInterpretation'
, calculationtime  path '$.exercises.loadInformation.calculationTime'
, sessiontype  path '$.exercises.loadInformation.calculationTime'
, sessionrpe  path '$.exercises.loadInformation.sessionRpe'
, perceivedload  path '$.exercises.loadInformation.perceivedLoad'
, perceivedloadinterpretation  path '$.exercises.loadInformation.perceivedLoadInterpretation'))  jt;
															
															
															
$.exercises	array	32767
$.exercises.ascent	number	8
$.exercises.autoLaps	array	8192
$.exercises.autoLaps.ascent	number	32
$.exercises.autoLaps.cadence	object	32
$.exercises.autoLaps.cadence.avg	number	2
$.exercises.autoLaps.cadence.max	number	4
$.exercises.autoLaps.descent	number	32
$.exercises.autoLaps.distance	number	8
$.exercises.autoLaps.duration	string	16
$.exercises.autoLaps.heartRate	object	32
$.exercises.autoLaps.heartRate.avg	number	4
$.exercises.autoLaps.heartRate.max	number	4
$.exercises.autoLaps.heartRate.min	number	4
$.exercises.autoLaps.lapNumber	number	2
$.exercises.autoLaps.power	object	32
$.exercises.autoLaps.power.avg	number	4
$.exercises.autoLaps.power.max	number	4
$.exercises.autoLaps.speed	object	64
$.exercises.autoLaps.speed.avg	number	32
$.exercises.autoLaps.speed.max	number	32
$.exercises.autoLaps.splitTime	string	16
$.exercises.cadence	object	32
$.exercises.cadence.avg	number	2
$.exercises.cadence.max	number	4
$.exercises.cadence.min	number	2
$.exercises.descent	number	8
$.exercises.distance	number	32
$.exercises.duration	string	16
$.exercises.heartRate	object	32
$.exercises.heartRate.avg	number	4
$.exercises.heartRate.max	number	4
$.exercises.heartRate.min	number	4
$.exercises.kiloCalories	number	4
$.exercises.laps	array	4096
$.exercises.laps.ascent	number	32
$.exercises.laps.cadence	object	32
$.exercises.laps.cadence.avg	number	2
$.exercises.laps.cadence.max	number	4
$.exercises.laps.descent	number	32
$.exercises.laps.distance	number	32
$.exercises.laps.duration	string	16
$.exercises.laps.heartRate	object	32
$.exercises.laps.heartRate.avg	number	4
$.exercises.laps.heartRate.max	number	4
$.exercises.laps.heartRate.min	number	4
$.exercises.laps.lapNumber	number	2
$.exercises.laps.power	object	32
$.exercises.laps.power.avg	number	4
$.exercises.laps.power.max	number	4
$.exercises.laps.speed	object	64
$.exercises.laps.speed.avg	number	32
$.exercises.laps.speed.max	number	32
$.exercises.laps.splitTime	string	16
$.exercises.latitude	number	32
$.exercises.loadInformation	object	512
$.exercises.loadInformation.cardioLoad	number	8
$.exercises.loadInformation.muscleLoad	number	8
$.exercises.loadInformation.sessionRpe	string	8
$.exercises.longitude	number	32
$.exercises.power	object	32
$.exercises.power.avg	number	4
$.exercises.power.max	number	4
$.exercises.samples	object	32767
$.exercises.samples.altitude	array	32767
$.exercises.samples.altitude.dateTime	string	32
$.exercises.samples.altitude.value	number	8
$.exercises.samples.cadence	array	32767
$.exercises.samples.cadence.dateTime	string	32
$.exercises.samples.cadence.value	number	4
$.exercises.samples.distance	array	32767
$.exercises.samples.distance.dateTime	string	32
$.exercises.samples.distance.value	number	32
$.exercises.samples.heartRate	array	32767
$.exercises.samples.heartRate.dateTime	string	32
$.exercises.samples.heartRate.value	number	4
$.exercises.samples.recordedRoute	array	32767
$.exercises.samples.speed	array	32767
$.exercises.samples.speed.dateTime	string	32
$.exercises.samples.speed.value	number	8
$.exercises.samples.temperature	array	32767
$.exercises.samples.temperature.dateTime	string	32
$.exercises.samples.temperature.value	number	4
$.exercises.speed	object	128
$.exercises.speed.avg	number	32
$.exercises.speed.max	number	32
$.exercises.speed.min	number	32
$.exercises.sport	string	32
$.exercises.startTime	string	32
$.exercises.stopTime	string	32
$.exercises.timezoneOffset	number	4
$.exercises.zones	object	2048
$.exercises.zones.heart_rate	array	512
$.exercises.zones.heart_rate.higherLimit	number	4
$.exercises.zones.heart_rate.inZone	string	8
$.exercises.zones.heart_rate.lowerLimit	number	4
$.exercises.zones.heart_rate.zoneIndex	number	1
$.exercises.zones.power	array	512
$.exercises.zones.power.higherLimit	number	4
$.exercises.zones.power.inZone	string	16
$.exercises.zones.power.lowerLimit	number	4
$.exercises.zones.power.zoneIndex	number	1
$.exercises.zones.speed	array	512
$.exercises.zones.speed.distance	number	8
$.exercises.zones.speed.higherLimit	number	8
$.exercises.zones.speed.inZone	string	16
$.exercises.zones.speed.lowerLimit	number	8
$.exercises.zones.speed.zoneIndex	number	1
$.exportVersion	string	4
$.feeling	string	32
$.kiloCalories	number	4
$.latitude	number	32
$.loadInformation	object	512
$.loadInformation.calculationTime	string	32
$.loadInformation.cardioLoad	number	8
$.loadInformation.muscleLoad	number	8
$.loadInformation.perceivedLoad	number	8
$.loadInformation.sessionRpe	string	8
$.longitude	number	32
$.maximumHeartRate	number	4
$.name	string	16
$.note	string	256
$.periodData	object	32767
$.periodData.attributes	array	64
$.periodData.attributes.key	string	16
$.periodData.attributes.textValue	string	8
$.periodData.end	string	32
$.periodData.start	string	32
$.periodData.subPeriods	array	32767
$.periodData.subPeriods.attributes	array	256
$.periodData.subPeriods.attributes.key	string	32
$.periodData.subPeriods.end	string	32
$.periodData.subPeriods.start	string	32
$.periodData.subPeriods.subPeriods	array	16384
$.periodData.subPeriods.subPeriods.end	string	32
$.periodData.subPeriods.subPeriods.start	string	32
$.periodData.subPeriods.subPeriods.type	string	8
$.periodData.subPeriods.type	string	8
$.periodData.type	string	16
$.physicalInformationSnapshot	object	512
$.physicalInformationSnapshot.birthday	string	16
$.physicalInformationSnapshot.dateTime	string	32
$.physicalInformationSnapshot.sex	string	8
$.physicalInformationSnapshot.sleepGoal	string	8
$.physicalInformationSnapshot.vo2Max	number	2
$.startTime	string	32
$.stopTime	string	32
$.timeZoneOffset	number	4
				   
select dj.userid, dj.device_id from polar_data d, json_table (d.polar_json, '$deviceDays.[*]'  NESTED LineItems[*] columns (userid path '$.userId', device_id path '$.deviceId')) dj;	

select dj.userid, dj.device_id from polar_data d, json_table (d.polar_json, '$.deviceDays'  columns (userid path '$.userId', device_id path '$.deviceId')) dj;


select dj.* from polar_data d,
json_table (d.polar_json, '$.deviceDays[*]'  columns (
  userid path '$.userId', 
  device_id path '$.deviceId',
  year path  '$.date.year',
  month path  '$.date.month',
  day path  '$.date.day',
   nested samples columns (heartRate, secondsFromDayStart, source))) dj;



{
  "type" : "object",
  "properties" :
  {
    "zip" :
    {
      "type" : "string",
      "o:length" : 1,
      "o:preferred_column_name" : "zip"
    },
    "city" :
    {
      "type" : "string",
      "o:length" : 8,
      "o:preferred_column_name" : "city"
    },
    "date" :
    {
      "type" : "string",
      "o:length" : 16,
      "o:preferred_column_name" : "date"
    },
    "motto" :
    {
      "type" : "string",
      "o:length" : 16,
      "o:preferred_column_name" : "motto"
    },
    "phone" :
    {
      "type" : "string",
      "o:length" : 1,
      "o:preferred_column_name" : "phone"
    },
    "state" :
    {
      "type" : "string",
      "o:length" : 1,
      "o:preferred_column_name" : "state"
    },
    "samples" :
    {
      "type" : "object",
      "o:length" : 32767,
      "o:preferred_column_name" : "samples",
      "properties" :
      {
        "mets" :
        {
          "type" : "array",
          "o:length" : 32767,
          "o:preferred_column_name" : "mets",
          "items" :
          {
            "properties" :
            {
              "value" :
              {
                "type" : "number",
                "o:length" : 8,
                "o:preferred_column_name" : "value"
              }
            }
          }
        },
        "steps" :
        {
          "type" : "array",
          "o:length" : 32767,
          "o:preferred_column_name" : "steps",
          "items" :
          {
            "properties" :
            {
              "value" :
              {
                "type" : "number",
                "o:length" : 4,
                "o:preferred_column_name" : "value"
              }
            }
          }
        },
        "metSources" :
        {
          "type" : "array",
          "o:length" : 16,
          "o:preferred_column_name" : "metSources",
          "items" :
          {
            "type" : "string",
            "o:length" : 8,
            "o:preferred_column_name" : "scalar_string"
          }
        }
      }
    },
    "street1" :
    {
      "type" : "string",
      "o:length" : 1,
      "o:preferred_column_name" : "street1"
    },
    "lastName" :
    {
      "type" : "string",
      "o:length" : 32,
      "o:preferred_column_name" : "lastName"
    },
    "nickname" :
    {
      "type" : "string",
      "o:length" : 16,
      "o:preferred_column_name" : "nickname"
    },
    "settings" :
    {
      "type" : "object",
      "o:length" : 1024,
      "o:preferred_column_name" : "settings",
      "properties" :
      {
        "language" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "language"
        },
        "timeZone" :
        {
          "type" : "string",
          "o:length" : 4,
          "o:preferred_column_name" : "timeZone"
        },
        "dateFormat" :
        {
          "type" : "string",
          "o:length" : 16,
          "o:preferred_column_name" : "dateFormat"
        },
        "newComment" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "newComment"
        },
        "newsletter" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "newsletter"
        },
        "timeFormat" :
        {
          "type" : "string",
          "o:length" : 16,
          "o:preferred_column_name" : "timeFormat"
        },
        "newFollower" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "newFollower"
        },
        "sessionLiked" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "sessionLiked"
        },
        "dateSeparator" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "dateSeparator"
        },
        "firstDayOfWeek" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "firstDayOfWeek"
        },
        "measurementUnit" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "measurementUnit"
        },
        "clubCommunication" :
        {
          "type" : "string",
          "o:length" : 4,
          "o:preferred_column_name" : "clubCommunication"
        },
        "profileVisibility" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "profileVisibility"
        },
        "activityVisibility" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "activityVisibility"
        },
        "guidanceOrArticles" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "guidanceOrArticles"
        },
        "myCommentCommented" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "myCommentCommented"
        },
        "receivedInvitation" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "receivedInvitation"
        },
        "timeFormatSeparator" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "timeFormatSeparator"
        },
        "activitySummaryLiked" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "activitySummaryLiked"
        },
        "activitySummaryCommented" :
        {
          "type" : "string",
          "o:length" : 2,
          "o:preferred_column_name" : "activitySummaryCommented"
        },
        "autoApproveFollowRequest" :
        {
          "type" : "string",
          "o:length" : 4,
          "o:preferred_column_name" : "autoApproveFollowRequest"
        },
        "trainingSessionVisibility" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "trainingSessionVisibility"
        }
      }
    },
    "username" :
    {
      "type" : "string",
      "o:length" : 32,
      "o:preferred_column_name" : "username"
    },
    "firstName" :
    {
      "type" : "string",
      "o:length" : 8,
      "o:preferred_column_name" : "firstName"
    },
    "deviceDays" :
    {
      "type" : "array",
      "o:length" : 32767,
      "o:preferred_column_name" : "deviceDays",
      "items" :
      {
        "properties" :
        {
          "date" :
          {
            "type" : "object",
            "o:length" : 64,
            "o:preferred_column_name" : "date",
            "properties" :
            {
              "day" :
              {
                "type" : "number",
                "o:length" : 2,
                "o:preferred_column_name" : "day"
              },
              "year" :
              {
                "type" : "number",
                "o:length" : 4,
                "o:preferred_column_name" : "year"
              },
              "month" :
              {
                "type" : "number",
                "o:length" : 2,
                "o:preferred_column_name" : "month"
              }
            }
          },
          "userId" :
          {
            "type" : "number",
            "o:length" : 8,
            "o:preferred_column_name" : "userId"
          },
          "samples" :
          {
            "type" : "array",
            "o:length" : 32767,
            "o:preferred_column_name" : "samples",
            "items" :
            {
              "properties" :
              {
                "source" :
                {
                  "type" : "string",
                  "o:length" : 16,
                  "o:preferred_column_name" : "source"
                },
                "heartRate" :
                {
                  "type" : "number",
                  "o:length" : 4,
                  "o:preferred_column_name" : "heartRate"
                },
                "secondsFromDayStart" :
                {
                  "type" : "number",
                  "o:length" : 8,
                  "o:preferred_column_name" : "secondsFromDayStart"
                }
              }
            }
          },
          "deviceId" :
          {
            "type" : "string",
            "o:length" : 8,
            "o:preferred_column_name" : "deviceId"
          }
        }
      }
    },
    "countryCode" :
    {
      "type" : "string",
      "o:length" : 2,
      "o:preferred_column_name" : "countryCode"
    },
    "exportVersion" :
    {
      "type" : "string",
      "o:length" : 4,
      "o:preferred_column_name" : "exportVersion"
    },
    "favouriteSports" :
    {
      "type" : "array",
      "o:length" : 32,
      "o:preferred_column_name" : "favouriteSports",
      "items" :
      {
        "type" : "string",
        "o:length" : 8,
        "o:preferred_column_name" : "scalar_string"
      }
    },
    "linkedApplications" :
    {
      "type" : "array",
      "o:length" : 2,
      "o:preferred_column_name" : "linkedApplications"
    },
    "linkedOrganisations" :
    {
      "type" : "array",
      "o:length" : 2,
      "o:preferred_column_name" : "linkedOrganisations"
    },
    "physicalInformation" :
    {
      "type" : "object",
      "o:length" : 256,
      "o:preferred_column_name" : "physicalInformation",
      "properties" :
      {
        "sex" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "sex"
        },
        "birthday" :
        {
          "type" : "string",
          "o:length" : 16,
          "o:preferred_column_name" : "birthday"
        },
        "sleepGoal" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "sleepGoal"
        },
        "height, cm" :
        {
          "type" : "number",
          "o:length" : 8,
          "o:preferred_column_name" : "height, cm"
        },
        "typicalDay" :
        {
          "type" : "string",
          "o:length" : 16,
          "o:preferred_column_name" : "typicalDay"
        },
        "weight, kg" :
        {
          "type" : "number",
          "o:length" : 4,
          "o:preferred_column_name" : "weight, kg"
        },
        "maximumHeartRate" :
        {
          "type" : "number",
          "o:length" : 4,
          "o:preferred_column_name" : "maximumHeartRate"
        },
        "restingHeartRate" :
        {
          "type" : "number",
          "o:length" : 2,
          "o:preferred_column_name" : "restingHeartRate"
        },
        "trainingBackground" :
        {
          "type" : "string",
          "o:length" : 8,
          "o:preferred_column_name" : "trainingBackground"
        },
        "speedCalibrationOffset" :
        {
          "type" : "number",
          "o:length" : 8,
          "o:preferred_column_name" : "speedCalibrationOffset"
        }
      }
    }
  }
}

