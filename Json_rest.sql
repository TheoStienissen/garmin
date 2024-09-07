insert into gmn_hydration
( uuid, person_id, version, vigorousintensityminutes, wellnessactivekilocalories, wellnessdistancemeters, wellnessendtimegmt                                          
, wellnessendtimelocal, wellnesskilocalories, wellnessstarttimegmt, wellnessstarttimelocal, wellnesstotalkilocalories                          
, calendardate, goalinml, lastentrytimestamplocal, sweatlossinml, valueinml, maxheartrate, minavgheartrate                                                   
, minheartrate, moderateintensityminutes, remainingkilocalories, avgwakingrespirationvalue, highestrespirationvalue                    
, latestrespirationtimegmt, latestrespirationvalue, lowestrespirationvalue, restingcaloriesfromactivity, restingheartrate                                                
, restingheartratetimestamp, totaldistancemeters, totalkilocalories, totalsteps, userintensityminutesgoal)
select uuid, person_id, version, vigorousintensityminutes, wellnessactivekilocalories, wellnessdistancemeters, date_test(wellnessendtimegmt) wellnessendtimegmt
, date_test(wellnessendtimelocal) wellnessendtimelocal, wellnesskilocalories, date_test(wellnessstarttimegmt) wellnessstarttimegmt,
date_test(wellnessstarttimelocal) wellnessstarttimelocal, wellnesstotalkilocalories
, to_date(respiration_calendardate, 'YYYY-MM-DD'), hydration_goalinml, date_test(hydration_lastentrytimestamplocal) hydration_lastentrytimestamplocal, 
hydration_sweatlossinml, hydration_valueinml, maxheartrate, minavgheartrate            
, minheartrate, moderateintensityminutes, remainingkilocalories, respiration_avgwakingrespirationvalue, respiration_highestrespirationvalue
, date_test(respiration_latestrespirationtimegmt) respiration_latestrespirationtimegmt, respiration_latestrespirationvalue, respiration_lowestrespirationvalue, restingcaloriesfromactivity, restingheartrate                           
, restingheartratetimestamp, totaldistancemeters, totalkilocalories, totalsteps, userintensityminutesgoal
from v_gmn_json_hydration
where uuid not in (select uuid from gmn_hydration);

update gmn_json_data set view_name =  'v_gmn_json_hydration'  where name like  '%UDSFile%' and view_name is null;
commit;

insert into gmn_biometic_profile (activity_class, threshold_power, height, vo2_max, vo2_max_cycling, weight, person_id)
select activityclass, functionalthresholdpower, height, vo2max, vo2maxcycling, weight, person_id from v_gmn_json_biometrics_profile
where (activityclass, functionalthresholdpower, height, vo2max, vo2maxcycling, weight, person_id)
not in (select activity_class, threshold_power, height, vo2_max, vo2_max_cycling, weight, person_id from gmn_biometic_profile);

update gmn_json_data set view_name =  'v_gmn_json_biometrics_profile'  where name like  '%userBioMetricProfileData%' and view_name is null;
commit;

insert into gmn_courses (courseName, courseType, createDate, elevationGain, elevationLoss, startpoint_elevation, startpoint_latitude, startpoint_longitude, update_date, person_id)
select  coursename,coursetype, to_date (substr(createdate,1, 10)|| substr (createdate, 12, 8), 'YYYY-MM-DDHH24:Mi:SS'), elevationgainmeter,
        elevationlossmeter, startpoint_elevation, startpoint_latitude, startpoint_longitude, 
        to_date (substr(updatedate,1, 10)|| substr (updatedate, 12, 8), 'yyyy-mm-ddhh24:mi:ss'), person_id
from v_gmn_json_courses
where courseName is not null
and (coursename,person_id) not in (select coursename,person_id from v_gmn_json_courses  group by coursename,person_id);

update gmn_json_data set view_name =  'v_gmn_json_courses'  where name like  '%courses%' and view_name is null;
commit;

insert into gmn_gear (gear_id, create_date, model, begin_date, display_name, status, max_meters, notified, updated, person_id)
select gearpk, to_date (createdate, 'YYYY-MM-DD'), custommakemodel, to_date (datebegin, 'YYYY-MM-DD'), displayname, gearstatusname,
 maximummeters, notified, to_date(updatedate, 'YYYY-MM-DD'), person_id
from v_gmn_json_gear
where (person_id, gearpk) not in (select person_id, gear_id from gmn_gear);

update gmn_json_data set view_name =  'v_gmn_json_gear'  where name like  '%gear%' and view_name is null;
commit;

insert into gmn_heartrate_zones (person_id, sport, trainingmethod, zone1, zone2, zone3, zone4, zone5, loaded)        
select distinct person_id, sport, trainingmethod, zone1floor, zone2floor, zone3floor, zone4floor, zone5floor, loaded from v_gmn_json_heartrate_zones
where (person_id, sport, trainingmethod, loaded) not in (select person_id, sport, trainingmethod, loaded from gmn_heartrate_zones);

update gmn_json_data set view_name =  'v_gmn_json_heartrate_zones'  where name like  '%heartRateZones%' and view_name is null;

insert into gmn_personal_records (person_id, record_type, confirmed, current_v, created, value)
select distinct person_id, personalrecordtype, confirmed, current_v, to_date(createddate, 'YYYY-MM-DD'), value from  v_gmn_json_personal_records
where personalrecordtype is not null
and (person_id, personalRecordType, current_v, value) not in (select person_id, record_type, current_v, value from gmn_personal_records);

update gmn_json_data set view_name =  'v_gmn_json_personal_records'  where name like  '%personalRecord%' and view_name is null;
commit;

insert into gmn_runrace_predictions (person_id, cal_date, racetime_5k, racetime_10k, half_marathon, marathon)
select person_id, to_date (calendardate, 'YYYY-MM-DD'), min(racetime5k), min(racetime10k), min(racetimehalf), min(racetimemarathon)
from v_gmn_json_runrace_predictions
where (person_id, to_date (calendardate, 'YYYY-MM-DD')) not in (select person_id, cal_date from gmn_runrace_predictions)
group by person_id, to_date (calendardate, 'YYYY-MM-DD');

update gmn_json_data set view_name =  'v_gmn_json_runrace_predictions'  where name like '%RunRacePredictions%' and view_name is null;
commit;

insert into gmn_user_profile( person_id, birthdate, email, first_name, last_name, gender, username)
select person_id, to_date(birthdate, 'YYYY-MM-DD'), emailaddress, firstname,lastname, gender,username from v_gmn_json_user_profile
where person_id not in (select person_id from gmn_user_profile);

update gmn_json_data set view_name =  'v_gmn_json_user_profile'  where name like  '%user_profile%' and view_name is null;
commit;

insert into gmn_user_settings (person_id, handedness, locale, loaded)
select person_id, handedness, preferredlocale, loaded from v_gmn_json_user_settings
where (person_id, loaded) not in (select person_id, loaded from gmn_user_settings);

update gmn_json_data set view_name =  'v_gmn_json_user_settings'  where name like  '%user_settings%' and view_name is null;
commit;

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
cal_date, sleep_end, sleep_start, person_id from v_gmn_json_sleep_data
where (person_id, cal_date) not in (select person_id, cal_date from gmn_sleep_data);  

update gmn_json_data set view_name =  'v_gmn_json_sleep_data'  where name like  '%sleepData%' and view_name is null;  
commit;

--
-- ToDo: duplicates on primary key
-- Problematic part
--
insert into gmn_training_readyness
( person_id, calendardate, device_id, acuteload, acwrfactorfeedback, acwrfactorpercent, feedbacklong, feedbackshort, hrvfactorfeedback                                               
, hrvfactorpercent, hrvweeklyaverage, ready_level, recoverytime, recoverytimefactorfeedback, recoverytimefactorpercent, score
, sleephistoryfactorfeedback, sleephistoryfactorpercent, sleepscore, sleepscorefactorfeedback, sleepscorefactorpercent
, stresshistoryfactorfeedback, stresshistoryfactorpercent, validsleep)
select person_id, to_date (calendardate, 'YYYY-MM-DD') calendardate, deviceid, acuteload, acwrfactorfeedback, acwrfactorpercent, feedbacklong, feedbackshort, hrvfactorfeedback                                                    
, hrvfactorpercent, hrvweeklyaverage, ready_level, recoverytime, recoverytimefactorfeedback, recoverytimefactorpercent, score
, sleephistoryfactorfeedback, sleephistoryfactorpercent, sleepscore, sleepscorefactorfeedback, sleepscorefactorpercent
, stresshistoryfactorfeedback, stresshistoryfactorpercent, validsleep
from v_gmn_json_training_readyness
where (person_id, to_date (calendardate, 'YYYY-MM-DD')) not in (select person_id, calendardate from gmn_training_readyness);

update gmn_json_data set view_name =  'v_gmn_json_training_readyness'  where name like '%TrainingReadinessDTO%' and view_name is null;
commit;

update gmn_json_data set view_name =  'v_gmn_json_TrainingHistory'  where name like 'TrainingHistory%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_metrics_metadata'  where name like 'MetricsMaxMetData_%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_AbnormalHrEvents'  where name like '%AbnormalHrEvents.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_EnduranceScore'  where name like 'EnduranceScore_%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_fitnessAgeData'  where name like '%fitnessAgeData.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_HillScore'  where name like 'HillScore%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_HeatAltitudeAcclimation'  where name like 'MetricsHeatAltitudeAcclimation%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_TrainingHistory'  where name like 'TrainingHistory%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_CalendarItems'  where name like 'CalendarItems.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_events'  where name like 'events.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_customer'  where name like 'customer.json' and view_name is null;
commit;
end;
/


begin 
for j in (select person_id, to_date (calendardate, 'YYYY-MM-DD') calendardate, deviceid, acuteload, acwrfactorfeedback, acwrfactorpercent, feedbacklong, feedbackshort, hrvfactorfeedback                                                    
            , hrvfactorpercent, hrvweeklyaverage, ready_level, recoverytime, recoverytimefactorfeedback, recoverytimefactorpercent, score
            , sleephistoryfactorfeedback, sleephistoryfactorpercent, sleepscore, sleepscorefactorfeedback, sleepscorefactorpercent
            , stresshistoryfactorfeedback, stresshistoryfactorpercent, validsleep
            from v_gmn_json_training_readyness
            where (person_id, to_date (calendardate, 'YYYY-MM-DD')) not in (select person_id, calendardate from gmn_training_readyness)
loop 
  begin
    insert into gmn_training_readyness
     ( person_id, calendardate, device_id, acuteload, acwrfactorfeedback, acwrfactorpercent, feedbacklong, feedbackshort, hrvfactorfeedback                                               
     , hrvfactorpercent, hrvweeklyaverage, ready_level, recoverytime, recoverytimefactorfeedback, recoverytimefactorpercent, score
     , sleephistoryfactorfeedback, sleephistoryfactorpercent, sleepscore, sleepscorefactorfeedback, sleepscorefactorpercent
     , stresshistoryfactorfeedback, stresshistoryfactorpercent, validsleep)
    values 
     ( j.person_id, j.calendardate, j.device_id, j.acuteload, j.acwrfactorfeedback, j.acwrfactorpercent, j.feedbacklong, j.feedbackshort, j.hrvfactorfeedback                                               
     , j.hrvfactorpercent, j.hrvweeklyaverage, j.ready_level, j.recoverytime, j.recoverytimefactorfeedback, j.recoverytimefactorpercent, j.score
     , j.sleephistoryfactorfeedback, j.sleephistoryfactorpercent, j.sleepscore, j.sleepscorefactorfeedback, j.sleepscorefactorpercent
     , j.stresshistoryfactorfeedback, j.stresshistoryfactorpercent, j.validsleep);
  
  exception when dup_val_on_index 
  then null;
  end;
end loop;





insert into gmn_training_readyness
( person_id, calendardate, device_id, acuteload, acwrfactorfeedback, acwrfactorpercent, feedbacklong, feedbackshort, hrvfactorfeedback                                               
, hrvfactorpercent, hrvweeklyaverage, ready_level, recoverytime, recoverytimefactorfeedback, recoverytimefactorpercent, score
, sleephistoryfactorfeedback, sleephistoryfactorpercent, sleepscore, sleepscorefactorfeedback, sleepscorefactorpercent
, stresshistoryfactorfeedback, stresshistoryfactorpercent, validsleep)
;