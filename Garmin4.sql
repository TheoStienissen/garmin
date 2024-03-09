create table gmn_biometic_profile
( activity_class     number (1)
, threshold_power    number (4)
, height             number (3)
, vo2_max            number (2)
, vo2_max_cycling    number (2)
, weight             number (6)
, log_date           date default sysdate
, person_id          number (4));

insert into gmn_biometic_profile (activity_class, threshold_power, height, vo2_max, vo2_max_cycling, weight, person_id)
select activityclass, functionalthresholdpower, height, vo2max, vo2maxcycling, weight, person_id from v_gmn_json_biometrics_profile;

create table            gmn_courses
( courseName            varchar2 (40)
, courseType            varchar2 (40)
, createDate            date
, elevationGain         number (4)
, elevationLoss         number (4)
, startpoint_elevation  number (4)
, startpoint_latitude	number
, startpoint_longitude  number
, update_date           date
, person_id             number (4));

alter table gmn_courses add constraint gmn_courses_pk primary key (person_id, courseName) using index;

insert into gmn_courses (courseName, courseType, createDate, elevationGain, elevationLoss, startpoint_elevation, startpoint_latitude, startpoint_longitude, update_date, person_id)
select  coursename,coursetype, to_date (substr(createdate,1, 10)|| substr (createdate, 12, 8), 'YYYY-MM-DDHH24:Mi:SS'), elevationgainmeter,
        elevationlossmeter, startpoint_elevation, startpoint_latitude, startpoint_longitude, 
        to_date (substr(updatedate,1, 10)|| substr (updatedate, 12, 8), 'yyyy-mm-ddhh24:mi:ss'), person_id
from v_gmn_json_courses
where courseName is not null
and (coursename,person_id, createdate) in (select coursename,person_id, max(createdate) from v_gmn_json_courses
   group by coursename,person_id);


create table gmn_gear
( gear_id          number (8)
, create_date     date
, model           varchar2 (40)
, begin_date      date
, display_name    varchar2 (40)
, status          varchar2 (10)
, max_meters      number (6)
, notified        varchar2 (10)
, updated         date
, person_id       number (4));

alter table gmn_gear add constraint gmn_gear_pk primary key (person_id, gear_id) using index;

insert into gmn_gear (gear_id, create_date, model, begin_date, display_name, status, max_meters, notified, updated, person_id)
select gearpk, to_date(createdate, 'YYYY-MM-DD'), custommakemodel, to_date(datebegin, 'YYYY-MM-DD'), displayname, gearstatusname,
 maximummeters, notified, to_date(updatedate, 'YYYY-MM-DD'), person_id
from v_gmn_json_gear;

create table gmn_heartrate_zones
( person_id       number (4)
, sport 	      varchar2(20)
, trainingmethod  varchar2(40)
, zone1           number (4)
, zone2           number (4)
, zone3           number (4)
, zone4           number (4)
, zone5           number (4));

alter table gmn_heartrate_zones add constraint gmn_heartrate_zones_pk primary key (person_id, sport, trainingmethod) using index;

insert into gmn_heartrate_zones ( person_id, sport , trainingmethod, zone1, zone2, zone3, zone4, zone5)        
select person_id, sport, trainingmethod, zone1floor, zone2floor, zone3floor, zone4floor, zone5floor from v_gmn_json_heartrate_zones;


create table gmn_personal_records
( person_id       number (4)
, record_type     varchar2 (40)
, confirmed       varchar2 (10)
, current_v       varchar2 (10)
, created         date
, value           number (10));

insert into gmn_personal_records (person_id, record_type, confirmed, current_v, created, value)
select distinct person_id, personalrecordtype, confirmed, current_v, to_date(createddate, 'YYYY-MM-DD'), value from  v_gmn_json_personal_records
where personalrecordtype is not null;

alter table gmn_personal_records add constraint gmn_personal_records_pk primary key (person_id, record_type, current_v, value) using index;

create table gmn_runrace_predictions
( person_id     number (4)
, cal_date      date
, racetime_5k   number (6)
, racetime_10k  number (6)
, half_marathon number (6)
, marathon      number (6));

insert into gmn_runrace_predictions (person_id, cal_date, racetime_5k, racetime_10k, half_marathon, marathon)
select person_id, to_date (calendardate, 'YYYY-MM-DD'), min(racetime5k), min(racetime10k), min(racetimehalf), min(racetimemarathon)
from v_gmn_json_runrace_predictions
group by person_id, to_date (calendardate, 'YYYY-MM-DD');

alter table gmn_runrace_predictions add constraint gmn_runrace_predictions_pk primary key (person_id, cal_date) using index;
 
create table gmn_user_profile
( person_id     number (4)
, birthdate     date
, email         varchar2 (40)
, first_name    varchar2 (20)
, last_name     varchar2 (40)
, gender        varchar2 (10)
, username      varchar2 (40));

insert into gmn_user_profile( person_id, birthdate, email, first_name, last_name, gender, username)
select person_id, to_date(birthdate, 'YYYY-MM-DD'), emailaddress, firstname,lastname, gender,username from v_gmn_json_user_profile;

alter  table gmn_user_profile add constraint gmn_user_profile_pk primary key (person_id) using index;

create table gmn_user_settings
( person_id     number (4)
, handedness    varchar2 (10)
, locale        varchar2 (10));

insert into gmn_user_settings ( person_id, handedness, locale)
select person_id, handedness, preferredlocale from v_gmn_json_user_settings;

alter table gmn_user_settings add constraint gmn_user_settings_pk primary key (person_id) using index;


-----------------
create table gmn_sleep_data
( avg_respiration             number (3)
, avgsleepstress              number(3)
, awakecount                  number(3)
, awakesleepseconds           number(6)
, calendardate                date
, deepsleepseconds            number(4)
, highestrespiration          number(4)
, lightsleepseconds           number(8)
, lowestrespiration           number(4)
, remsleepseconds             number(4)
, restlessmomentcount         number(4)
, sleep_end_gmt               date
, awaketimescore              number(2)
, awakeningscountscore        number(2)
, combinedawakescore          number(2)
, deepscore                   number(2)
, durationscore               number(4)
, feedback                    varchar2(40)
, insight                     varchar2(40)
, interruptionsscore          number(2)
, lightscore                  number(2)
, overallscore                number(2)
, qualityscore                number(2)
, recoveryscore               number(4)
, remscore                    number(4)
, restfulnessscore            number(2)
, sleepstarttimestampgmt      varchar2(40)
, sleepwindowconfirmationtype varchar2(40)
, unmeasurableseconds         number(1)
, cal_date                    date
, sleep_end                   date
, sleep_start                 date
, person_id                   number(4));


-- ToDo
insert into gmn_sleep_data 
( avg_respiration, avgsleepstress, awakecount, awakesleepseconds, calendardate, deepsleepseconds, highestrespiration, lightsleepseconds           
, lowestrespiration, remsleepseconds, restlessmomentcount, sleep_end_gmt, awaketimescore, awakeningscountscore, combinedawakescore          
, deepscore, durationscore, feedback, insight, interruptionsscore, lightscore, overallscore, qualityscore, recoveryscore, remscore                    
, restfulnessscore, sleepstarttimestampgmt, sleepwindowconfirmationtype, unmeasurableseconds, cal_date, sleep_end, sleep_start, person_id)
select averagerespiration, avgsleepstress, awakecount, awakesleepseconds, to_date(calendardate, 'YYYY-MM-DD'), deepsleepseconds, highestrespiration, lightsleepseconds,
lowestrespiration, remsleepseconds, restlessmomentcount,
 to_date (substr (sleependtimestampgmt, 1, 10) || substr (sleependtimestampgmt, 12, 8), 'YYYY-MM-DDHH24:MI:SS'),
 sleepscores_awaketimescore, sleepscores_awakeningscountscore,
sleepscores_combinedawakescore, sleepscores_deepscore, sleepscores_durationscore, sleepscores_feedback, sleepscores_insight,
sleepscores_interruptionsscore, sleepscores_lightscore, sleepscores_overallscore, sleepscores_qualityscore, sleepscores_recoveryscore,
sleepscores_remscore, sleepscores_restfulnessscore, sleepstarttimestampgmt, sleepwindowconfirmationtype, unmeasurableseconds,
cal_date, sleep_end, sleep_start, person_id from  v_gmn_json_sleep_data;           



 
 V_GMN_JSON_TRAINING_READYNESS
 
 V_GMN_SESSION_INFO_STEP_LENGTH
 
 V_GMN_JSON_HYDRATION
 
 
create table gmn_logbook
( id         integer generated always as identity
, user_id    integer (6)
, log_date   date default sysdate,
, picture    blob 
, text       varchar2 (2000));

alter table gmn_logbook add constraint gmn_logbook_pk primary key (id) using index;
alter table gmn_logbook add constraint gmn_logbook_fk1 foreign key (user_id) references gmn_users (id) on delete cascade;

