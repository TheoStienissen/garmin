DOC

  Author   :  Theo Stienissen
  Date     :  October  2023
  Changed  :  December 2023
  Purpose  :  Analyze data from Garmin watch in an Oracle database
  Status   :  Production Acceptance
  Contact  :  theo.stienissen@gmail.com
  @C:\Users\Theo\OneDrive\Theo\Project\garmin\garmin3.sql

Garmin data export: https://www.garmin.com/en-GB/account/datamanagement/exportdata/

There are 3 possible data sources to this solution:
1. Downloaded CSV from the activities page: activities.csv This data is loaded in the GARMIN table.
   Column names are in Dutch, because the setting on my watch are in Dutch.
2. Downloaded CSV from the activity itself. This data is loaded in the GARMIN_DETAILS table.
3. The .fit files which can be copied from the watch. Directory:  This PC\fenix 7\Internal Storage\GARMIN\Activity
In this application I have used exclusively the third option: .fit files

Dependencies:
  1. util errorhandling package
  2. Pipelined function get_file_name in the SYS schema
  3. external job scheduler service is running
  4. Authentication function my_authentication and table my_users
  5. Package blob_pkg
  6. Privilleges to run scheduler jobs (scheduler_admin)
  7. select privilege on dba_directories
  
Todo:


#

create or replace type text_row as object (training0 varchar2 (50),
  training1 varchar2  (50) , training2 varchar2 (50), training3  varchar2 (50), training4  varchar2 (50), training5  varchar2 (50),
  training6 varchar2  (50) , training7 varchar2 (50), training8  varchar2 (50), training9  varchar2 (50), training10 varchar2 (50),
  training11 varchar2 (50), training12 varchar2 (50), training13 varchar2 (50), training14 varchar2 (50), training15 varchar2 (50),
  training16 varchar2 (50), training17 varchar2 (50), training18 varchar2 (50), training19 varchar2 (50), training20 varchar2 (50));
  
create or replace type text_tab as table of text_row;
/

create or replace type int_row as object (exercise_date date,
  training1  number (10, 2), training2  number (10, 2), training3  number (10, 2), training4  number (10, 2), training5  number (10, 2),
  training6  number (10, 2), training7  number (10, 2), training8  number (10, 2), training9  number (10, 2), training10 number (10, 2),
  training11 number (10, 2), training12 number (10, 2), training13 number (10, 2), training14 number (10, 2), training15 number (10, 2),
  training16 number (10, 2), training17 number (10, 2), training18 number (10, 2), training19 number (10, 2), training20 number (10, 2));
/ 
  
create or replace type int_tab as table of int_row;
/

-- drop table gmn_users;

-- The avatar or nick_name needs to be unique
create table gmn_users
( id            integer generated always as identity
, first_name    varchar2 (20)
, last_name     varchar2 (50)
, nick_name     varchar2 (20) not null
, hr_low        number (3)    not null default 100
, hr_medium     number (3)    not null default 120
, hr_high       number (3)    not null default 140
, longitude     number -- Home location
, lattitude     number
, avatar        blob
, fit_directory varchar2 (10));

insert into gmn_users (first_name, last_name, nick_name) values ('Theo'   , 'Stienissen', 'Theo');
insert into gmn_users (first_name, last_name, nick_name) values ('Dolly'  , 'Stienissen', 'Dolly');
insert into gmn_users (first_name, last_name, nick_name) values ('Celeste', 'Stienissen', 'Celeste');

alter table gmn_users add constraint gmn_users_pk primary key (id);
create unique index gmn_users_uk1 on gmn_users (nick_name);

create table gmn_devices
( id            number (6)  generated always as identity
, user_id       number (6)
, description   varchar2 (50)
, serial#       number (12));

alter table gmn_devices add constraint gmn_devices_pk primary key (id);
alter table gmn_devices add constraint gmn_devices_fk1 foreign key (user_id) references gmn_users (id) on delete cascade;
create unique index gmn_devices_uk1 on gmn_devices (serial#);

insert into gmn_devices (user_id, description, serial#) values (1,'Garmin watch Theo', 3426042514);
insert into gmn_devices (user_id, description, serial#) values (2,'Garmin watch Dolly', 3446481735);

create table gmn_fit_routines
( id      integer generated always as identity
, routine varchar2 (50));

insert into gmn_fit_routines (routine) values ('FitToCSV-session.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV-record.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV-lap.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV.bat');
insert into gmn_fit_routines (routine) values ('FitToCSV-data.bat');

create table gmn_fit_files
( id        number generated always as identity
, user_id   integer
, file_name varchar2 (50)
, training  number (1)
, track     number (1));

alter table gmn_fit_files add constraint gmn_fit_files_pk  primary key (user_id, file_name);
alter table gmn_fit_files add constraint gmn_fit_files_fk1 foreign key (user_id) references gmn_users (id) on delete set null;
create unique index gmn_fit_files_uk1 on gmn_fit_files (id);

create table gmn_csv_by_column_name
( file_id      number
, field        varchar2 (50)
, val          varchar2 (60));

create table gmn_csv_by_field_name
( id           number generated always as identity
, file_id      number
, field        varchar2 (50)
, val          varchar2 (60)
, unit         varchar2 (50));

create directory garmin               as 'C:\Work\garmin';
create directory garmin_backup        as 'C:\Work\garmin\backup';
-- Users
create or replace directory fit_dolly   as 'C:\Work\garmin\fit_dolly';
create or replace directory fit_theo    as 'C:\Work\garmin\fit_theo';
create or replace directory fit_celeste as 'C:\Work\garmin\fit_celeste';

create table gmn_session_info (
fit_id                           integer,
avg_heart_rate                   number (3),
max_heart_rate                   number (3),
enhanced_avg_speed               number (5, 2),
enhanced_max_speed               number (5, 2),
sport                            varchar2 (50),
sport_profile_name               varchar2 (50), 
start_time                       date,
start_position_lat               number,
start_position_long              number,
end_position_lat                 number,
end_position_long                number,
total_training_effect            number (5, 2),
total_anaerobic_training_effect  number (5, 2),
total_ascent                     number (6, 2),
total_descent                    number (6, 2),
total_distance                   number (7, 2),
total_calories                   number (4),
total_elapsed_time               number (9, 3),
total_fractional_ascent          number (5, 2),
total_fractional_descent         number (5, 2),
total_timer_time                 number (7, 2),
training_load_peak               number (9, 3),
avg_power                        number (5, 2),
avg_step_length                  number (8, 2),
avg_vertical_oscillation         number (4, 2),
avg_vertical_ratio               number (4, 2),
max_fractional_cadence           number (4, 2),
max_power                        number (4),
max_running_cadence              number (4),
normalized_power                 number (4),
total_strides                    number (5),
total_work                       number (7, 0));

alter table gmn_session_info add constraint gmn_session_info_pk  primary key (fit_id);
alter table gmn_session_info add constraint gmn_session_info_fk1 foreign key (fit_id) references gmn_fit_files (id) on delete set null;

-- 5. Create a table with all the attribute fields that we are interested in
create table gmn_fit_data
( fit_id               number (6)
, id                   number (10)
, person_id            number (4)
, avg_heart_rate       number (3)
, avg_power            number (3)
, max_power            number (3)
, step_length          number (5, 1)
, avg_step_length      number (5, 1)
, distance             number (9, 2)
, total_distance       number (9, 2)
, enhanced_avg_speed   number (6, 3)
, enhanced_speed       number (6, 3)
, heart_rate           number (3)
, max_heart_rate       number (3)
, position_lat         number
, position_long        number
, power                number (3)
, start_position_lat   number
, start_position_long  number
, start_time           date
, timestamp            date
, total_training_effect number (2, 1)
, total_work           number (7)
, enhanced_respiration_rate number(4,2)
, threshold_heart_rate number(3,0)
, enhanced_altitude	   number(6,2)    
, total_timer_time	   number(8,3)
, end_position_lat	   number
, end_position_long	   number
, total_calories	   number(5,0)  
, total_elapsed_time   number(8,3) 
, enhanced_max_speed   number(5,3)
, name	               varchar2 (150)
, total_ascent     	   number(5,1)  
, total_descent	       number(5,1) 
, enhanced_max_altitude number(6,2)             
, num_laps	           number(3,0)
, cadence              number (3)
, time_in_hr_zone      varchar2 (100)
, avg_cadence          number (3)
, total_strides        number (6)
, sport_profile_name   varchar2 (100)
)
 partition by list (fit_id) automatic
(partition p_1 values (1));


alter table gmn_fit_data add constraint gmn_fit_data_pk  primary key (fit_id, id, person_id);
alter table gmn_fit_data add constraint gmn_fit_data_fk1 foreign key (person_id) references gmn_users (id);

create table gmn_json_data
( id   integer generated always as identity
, person_id   number (6) not null
, name varchar2 (100)
, json_doc  blob
, json_clob clob);

alter table gmn_json_data add constraint test_json_ck1 check (json_doc is json);
alter table gmn_json_data add constraint test_json_ck2 check (json_clob is json); 
alter table gmn_json_data add constraint gmn_json_data_fk1 foreign key (person_id) references gmn_users (id) on delete cascade;

create table gmn_upload_file
( id        integer generated always as identity,
  fitfile   blob,
  filename  varchar2(250),
  mimetype  varchar2(250),
  charset   varchar2(250),
  created   date default sysdate,
  last_update date);
  
alter table gmn_upload_file add constraint gmn_upload_file_pk primary key (id) using index;

create or replace trigger gmn_upload_file_bri
before insert on gmn_upload_file
for each row 
begin
  if :new.filename like '%[%]%'
  then :new.filename := substr (:new.filename, 1, instr (:new.filename, '[') - 1) || substr (:new.filename, instr (:new.filename, ']') + 1);
  end if;
  blob_pkg.blob_to_file (:new.fitfile, 'GARMIN', :new.filename);
end;
/

create table gmn_logbook
( id         integer generated always as identity
, user_id    number (6) not null
, created    date
, updated    date
, picture    blob 
, text       varchar2 (2000));

alter table gmn_logbook add constraint gmn_logbook_pk primary key (id) using index;
alter table gmn_logbook add constraint gmn_logbook_fk1 foreign key (user_id) references gmn_users (id) on delete cascade;

create or replace trigger gmn_logbook_briu
before insert or update on gmn_logbook
for each row 
begin 
  if inserting
  then :new.created := sysdate;
  else :new.updated := sysdate;
  end if;
  if :new.picture is null
  then select u.avatar into :new.picture from gmn_users u where u.id = :new.user_id;
  end if;
end gmn_logbook_briu;
/

-- https://www.youtube.com/watch?v=JzDsvuEBRaU
 

-- Step length
create or replace view v_gmn_session_info_step_length 
as
  select u.id person_id, u.nick_name, si.fit_id, si.sport_profile_name, si.avg_step_length, si.avg_heart_rate, si.max_heart_rate, si.start_time, si.total_distance, 
si.total_elapsed_time, 3.6 * si.enhanced_avg_speed speed
from gmn_session_info si, gmn_fit_files ff, gmn_users u
where ff.id = si.fit_id and ff.user_id = u.id and si.avg_step_length is not null;

create table gmn_sport_profiles
( id                  integer generated always as identity
, sport_profile_name  varchar2 (50)
, apex_color          varchar2 (50));

create or replace view gmn_calendar_info 
as
select si.fit_id, ff.user_id, u.nick_name || ': ' || sp.sport_profile_name title, si.start_time, si.start_time + (si.total_elapsed_time /3600/24) end_time, sp.sport_profile_name, sp.apex_color css_class
from gmn_session_info si
join gmn_fit_files ff on (si.fit_id = ff.id)
join gmn_users u on (u.id = ff.user_id)
join gmn_sport_profiles sp on (sp.sport_profile_name = si.sport_profile_name);

set serveroutput on size unlimited

create or replace package garmin_pkg
is
g_max_int   constant integer := power (2, 31);

function  extract_filename (p_filename in varchar2) return varchar2;

function  change_extention (p_filename in varchar2, p_new_extention in varchar2) return varchar2;

function  directory_path (p_directory in varchar2 default 'GARMIN') return varchar2;

procedure set_nls;

function  ds_to_varchar2 (p_ds in interval day to second) return varchar2;

function  to_nr (p_string in varchar2, p_col in varchar2 default null) return number;

function  to_ds (p_string in varchar2, p_col in varchar2 default null) return interval day to second;

function  to_dt (p_string in varchar2, p_col in varchar2 default null) return date;

function  interval_ds_to_seconds (p_interval in interval day to second) return integer;

function  seconds_to_ds_interval (p_seconds in integer) return interval day to second;

function  date_offset_to_date (p_offset in integer) return date;

function  semicircles_to_lon_lat (p_semicircle in integer) return number;

procedure sync_fit_files (p_directory in varchar2 default 'GARMIN');

procedure convert_fit_file_to_csv (p_file_name in varchar2, p_routine in varchar2);

procedure parse_csv_by_column_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN');

procedure parse_csv_by_field_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN', p_skip1 boolean default true);

procedure load_session_info (p_directory in varchar2 default 'GARMIN');

procedure load_session_details (p_directory in varchar2 default 'GARMIN');

procedure remove_csv_files (p_directory in varchar2 default 'GARMIN');

function  get_heartrate (p_person_id in integer, p_range in integer) return integer;

function  fit_data_loaded (p_fit_id in integer) return integer;

procedure reload_fit_data (p_fit_id in integer);

function  analyze_data (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 40) return text_tab pipelined;

function  analyze_graph (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 20) return int_tab pipelined;

procedure load_json_files (p_person_id in integer, p_directory in varchar2 default 'GARMIN');

procedure archive_fit_files (p_retention_days in integer default 7);

end garmin_pkg;
/


create or replace package body garmin_pkg
is

--
-- Strip filename from a path / string. Cut after the last slash
--
function extract_filename (p_filename in varchar2) return varchar2
is
l_filename varchar2 (200) := p_filename;
begin
  if    instr (p_filename , chr(92)) > 0  -- Windows
  then  select substr (p_filename, - instr (reverse (p_filename), chr(92)) + 1) into l_filename from dual;
  elsif instr (p_filename , chr(47)) > 0  -- Linux
  then  select substr (p_filename, - instr (reverse (p_filename), chr(47)) + 1) into l_filename from dual;
  end if;
  return l_filename;
 
exception when others then
  util.show_error ('Error in function extract_filename for: ' || p_filename, sqlerrm);
  return null;
end extract_filename;
 
/******************************************************************************************************************************************************************/

--
-- Convert file_type of a file. E.g.: <fn>.txt --> <fn>.csv
--
function change_extention (p_filename in varchar2, p_new_extention in varchar2) return varchar2
is
l_filename varchar2 (200) := p_filename;
begin
  if instr (p_filename, '.') > 0
  then
    select substr (p_filename, 1, length (p_filename) - instr (reverse (p_filename), '.') + 1) || p_new_extention into l_filename from dual;
  end if;
  return l_filename;
 
exception when others then
  util.show_error ('Error in function change_extention for: ' || p_filename || ' and extention ' || p_new_extention, sqlerrm);
  return null;
end change_extention;
 
/******************************************************************************************************************************************************************/

--
-- Convert file_type of a file. E.g. <fn>.txt --> <fn>.csv
--
function directory_path (p_directory in varchar2 default 'GARMIN') return varchar2
is
l_directory_path varchar2 (200);
begin
  select directory_path || chr (92) into l_directory_path from dba_directories where directory_name = upper(p_directory);
  return l_directory_path;  

exception when others then
  util.show_error ('Error in function directory_path for directory: ' || p_directory, sqlerrm);
end directory_path; 
  
/******************************************************************************************************************************************************************/

--
-- Set the NLS environment
--
procedure set_nls
is
begin
execute immediate 'alter session set NLS_DATE_FORMAT = ''YYYY/MM/DD HH24:MI:SS''';
execute immediate 'alter session set NLS_NUMERIC_CHARACTERS = ''.,''';

exception when others then
  util.show_error ('Error in procedure set_nls', sqlerrm);
end set_nls;

/******************************************************************************************************************************************************************/

--
-- Convert "interval day to second" to a standard format string
--
function ds_to_varchar2 (p_ds in interval day to second) return varchar2
is 
begin 
  return lpad (extract (day from p_ds ) , 2, '0') || ' ' || lpad (extract (hour from p_ds), 2, '0') || ':' || lpad (extract (minute from p_ds), 2, '0')  || ':' || lpad (extract (second from p_ds), 2, '0');

exception when others then
  util.show_error ('Error in function ds_to_varchar2', sqlerrm);
  return null;
end ds_to_varchar2;

/******************************************************************************************************************************************************************/

--
-- Convert the different number (or tempo) formats
--
function to_nr (p_string in varchar2, p_col in varchar2 default null) return number
is 
l_nr    number (20,4);
begin
  if    p_string is null or p_string = '--' then return null;
  elsif p_string like '%:%'    -- return tempo 
  then  l_nr := round (3600 / (60 * substr (p_string, 1, instr (p_string, ':') - 1) + substr (p_string, instr (p_string, ':') + 1)), 2);  
  else  l_nr := to_number (replace (p_string, ',', '.'));  
  end if;
  return l_nr;

exception when zero_divide then return null;
when others then
  util.show_error ('Error in procedure to_nr. Not a number: ' || p_col || '  ' || p_string , sqlerrm);
  return null;
end to_nr;

/******************************************************************************************************************************************************************/

--
-- To_dsinterval for different formats
--
function to_ds (p_string in varchar2, p_col in varchar2 default null) return interval day to second
is
l_ds interval day to second;
begin
  if    p_string is null or p_string = '--'  then l_ds := null;
  elsif instr (p_string, ':') = 0            then l_ds :=  to_dsinterval ( '00 00:00:' || replace (p_string, ',', '.'));
  elsif instr (p_string, ':', 1, 2) = 0      then l_ds :=  to_dsinterval ( '00 00:'    || replace (p_string, ',', '.'));
  else  l_ds := to_dsinterval ( '00 ' || replace (p_string, ',', '.'));
  end if;
  return l_ds;
  
exception when others then
  util.show_error ('Not an interval: ' || p_col || '  ' || p_string, sqlerrm);
  return null;
end to_ds;

/******************************************************************************************************************************************************************/

--
-- Different date formats are used by the watch
--
function to_dt (p_string in varchar2, p_col in varchar2 default null) return date
is
l_date  date;
begin
  set_nls;
  begin
    if   instr (p_string, ':', 1, 2) > 0
    then l_date := to_date (p_string, 'YYYY-MM-DD HH24:MI:SS');
    else l_date := to_date (p_string, 'YYYY-MM-DD HH24:MI');
    end if;
    return l_date;
    exception when others
    then
      if   instr (p_string, ':', 1, 2) > 0
      then l_date := to_date (p_string, 'MM/DD/YYYY HH24:MI:SS');
      else l_date := to_date (p_string, 'MM/DD/YYYY HH24:MI');
      end if;
      return l_date;    
  end;

exception when others then
  util.show_error ('Error in function to_dt for column: ' || p_col || '. Not a date or unknown format: ' || p_string, sqlerrm);
  return null;
end to_dt;

/******************************************************************************************************************************************************************/

--
-- Convert interval DS to seconds
--
function interval_ds_to_seconds (p_interval in interval day to second) return integer
is 
begin 
  return 86400 * extract (day from p_interval) + 3600 * extract (hour from p_interval) + 60 * extract (minute from p_interval) + extract (second from p_interval);

exception when others then 
   util.show_error ('Error in function interval_ds_to_seconds', sqlerrm);
   return null;
end interval_ds_to_seconds;

/******************************************************************************************************************************************************************/

--
-- Convert seconds to interval day to second
--
function seconds_to_ds_interval (p_seconds in integer) return interval day to second
is 
l_tot_seconds integer (6) := floor (p_seconds);
l_hours       integer (2);
l_minutes     integer (2);
l_seconds     integer (2);
l_fraction    integer (4);
l_interval    varchar2 (20);
begin
  if p_seconds is null then return null; end if;
  l_hours    := floor ( l_tot_seconds / 3600);
  l_minutes  := floor ((l_tot_seconds - l_hours * 3600) / 60);
  l_seconds  := mod (l_tot_seconds, 60); 
  l_fraction := round ((p_seconds - l_tot_seconds) * 100, 2);
  l_interval := '00 ' || lpad (to_char (l_hours), 2, '0')  || ':' || lpad (to_char (l_minutes), 2, '0')  || ':' || lpad (to_char (l_seconds), 2, '0') || '.' || lpad (to_char (l_fraction), 2, '0');
  return to_dsinterval (l_interval);

exception when others then 
   util.show_error ('Error in function seconds_to_ds_interval for: ' || p_seconds || ' : ' || l_interval, sqlerrm);
   return null;
end seconds_to_ds_interval;

/******************************************************************************************************************************************************************/

--
-- Number of seconds that have past since the 31-st of Dec 1989. Correction for dst is still required?
--
function date_offset_to_date (p_offset in integer) return date
is
begin
  return to_date ('31-12-1989', 'DD-MM-YYYY') + p_offset / 3600 / 24;

exception when others then 
   util.show_error ('Error in function date_offset_to_date for: ' || p_offset, sqlerrm);
   return null;
end date_offset_to_date;

/******************************************************************************************************************************************************************/

--
-- Convert simicircles to degrees longitude or lattitude
--
function semicircles_to_lon_lat (p_semicircle in integer) return number
is
begin 
  return p_semicircle * (180 / g_max_int);

exception when others then 
   util.show_error ('Error in function semicircles_to_lon_lat for: ' || p_semicircle, sqlerrm);
   return null;
end semicircles_to_lon_lat;

/******************************************************************************************************************************************************************/

--
-- Sync_fit_files
--
procedure sync_fit_files (p_directory in varchar2 default 'GARMIN')
is
begin
  insert into gmn_fit_files (file_name)
      select garmin_pkg.extract_filename (file_name) from table (get_file_name (garmin_pkg.directory_path (p_directory), 'fit'))
	    where garmin_pkg.extract_filename (file_name) not in (select file_name from gmn_fit_files) order by 1 desc;		
  commit;

exception when others then 
   util.show_error ('Error in procedure sync_fit_files', sqlerrm);
end sync_fit_files;

/******************************************************************************************************************************************************************/

-- 
-- The Oracle scheduler job is able to run external routines. Requirement is that the external job scheduler service is running
-- https://developer.garmin.com/fit/download/
-- https://developer.garmin.com/fit/protocol/
--
procedure convert_fit_file_to_csv (p_file_name in varchar2, p_routine in varchar2)
is
l_job_name    varchar2 (100) := dbms_scheduler.generate_job_name;
l_action      varchar2 (200) := 'C:\Work\garmin_bu\FitSDKRelease_21.115.00\FitSDKRelease_21.115.00\java' || chr(92) || p_routine;
begin 
   dbms_scheduler.create_job (job_name    => l_job_name,
                              job_type    => 'executable',
                              job_action  => l_action,
                              number_of_arguments => 1,
                              auto_drop   => true);
   dbms_scheduler.set_job_argument_value (l_job_name, 1, p_file_name);
   dbms_scheduler.run_job  (l_job_name);
   dbms_scheduler.drop_job (l_job_name);
   commit;

exception when others then
   dbms_scheduler.drop_job (l_job_name);
   commit;
   util.show_error ('Error in procedure convert_fit_file_to_csv for: ' || p_file_name || '. Please check if the Oracle job scheduler service is running.', sqlerrm);
end convert_fit_file_to_csv;

/******************************************************************************************************************************************************************/

--
-- Parse_csv_by_column_name
--
procedure parse_csv_by_column_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN')
is
type string_ty   is table of varchar2 (4000) index by binary_integer;
l_column_header  string_ty;
l_bfile          bfile;
l_last           number := 1;
l_current        number;
l_string         varchar2 (32767);
l_next           varchar2 (100);
l_cnt            integer := 0;
l_first          boolean := TRUE;
--
function next_item return varchar2 
is
l_pos    integer;
l_return varchar2 (100);
  begin
	if substr (l_string, 1, 1) = ',' then l_string := substr (l_string, 2); end if;
	if substr (l_string, 1, 1) = '"'
	then 
	  l_pos    := instr  (l_string, '"', 2, 1);
	  l_return := substr (l_string, 2, l_pos - 2);
	else
	  l_pos    := instr  (l_string, ',');
	  l_return := substr (l_string, 1, l_pos - 1);
	end if;
      l_string := substr (l_string, l_pos + 1);	
	return l_return;
	
  exception when others then
    util.show_error ('Error in function next_item', sqlerrm);
    return null;
  end next_item;
--
begin
  execute immediate 'truncate table gmn_csv_by_column_name';
  l_bfile := bfilename (p_directory, p_csv_file);
  if dbms_lob.fileexists (l_bfile) = 1
  then
    dbms_lob.fileopen (l_bfile);  
    loop
      l_current := dbms_lob.instr (l_bfile, '0A', l_last, 1 );
      exit when nvl (l_current, 0) = 0;
      l_string  := utl_raw.cast_to_varchar2 (dbms_lob.substr (l_bfile, l_current - l_last, l_last));
  
    if l_first
    then 
      loop
        l_cnt := l_cnt + 1;
        l_next    := next_item;
        l_column_header (l_cnt) := replace (l_next, 'session.');
        exit when nvl (instr (l_string, ','), 0) = 0;
      end loop;
      l_first := FALSE;
    else
      for j in 1 .. l_cnt
      loop 
        l_next := next_item;
        insert into gmn_csv_by_column_name (file_id, field, val) values (p_file_id, l_column_header (j), l_next);
      end loop;
    end if;
    l_last := l_current + 1;
    end loop;
    dbms_lob.close (l_bfile);
    commit;
  else
    raise_application_error (-20001, 'Error in procedure parse_csv_by_column_name. File ' || p_csv_file || ' does not exist');
  end if;

exception when others then
    util.show_error ('Error in procedure parse_csv_by_column_name for file ID: ' || p_file_id, sqlerrm);
end parse_csv_by_column_name;

/******************************************************************************************************************************************************************/

--
-- Parse_csv_by_field_name
--
procedure parse_csv_by_field_name (p_csv_file in varchar2, p_file_id in integer, p_directory in varchar2 default 'GARMIN', p_skip1 boolean default true)
is
l_bfile          bfile;
l_last           number := 1;
l_current        number;
l_string         varchar2 (32767);
l_rec1           varchar2 (1000);
l_rec2           varchar2 (1000);
l_rec3           varchar2 (1000);
--
function next_item return varchar2 
is
l_pos    integer;
l_return varchar2 (1000);
  begin
	if substr(l_string, 1, 1) = ',' then l_string := substr (l_string, 2); end if;
	if substr (l_string, 1, 1) = '"'
	then 
	  l_pos    := instr  (l_string, '"', 2, 1);
	  l_return := substr (l_string, 2, l_pos - 2);
	else
	  l_pos    := instr  (l_string, ',');
	  l_return := substr (l_string, 1, l_pos - 1);
	end if;
      l_string := substr (l_string, l_pos + 1);	
	return l_return;
	
  exception when others then
    util.show_error ('Error in function next_item', sqlerrm);
    return null;
  end next_item;
--
begin
  execute immediate 'truncate table gmn_csv_by_field_name';
  l_bfile  := bfilename (p_directory, p_csv_file);
  if dbms_lob.fileexists (l_bfile) = 1
  then
    dbms_lob.fileopen (l_bfile);
    if p_skip1  -- Skip the first line?
    then
      l_current := dbms_lob.instr (l_bfile, '0A', l_last, 1 );
      l_last := l_current + 1;
    end if;
  
    loop
      l_current := dbms_lob.instr (l_bfile, '0A', l_last, 1 );
      exit when nvl (l_current, 0) = 0;
      l_string := utl_raw.cast_to_varchar2 (dbms_lob.substr (l_bfile, l_current - l_last, l_last));
  
      loop
        l_rec1 := next_item;
        l_rec2 := next_item;
        l_rec3 := next_item;
        if l_rec1 not like '%Data%' and l_rec1 not like '%Definition%' and l_rec1 not like '%unknown%' and l_rec1 not like 'Field%' and l_rec1 is not null
        then
          insert into gmn_csv_by_field_name (file_id, field, val, unit) values (p_file_id, l_rec1, l_rec2, l_rec3);
        end if;
        exit when nvl(instr (l_string, ','), 0) = 0;
      end loop;
      l_last := l_current + 1;
    end loop;
    commit;
  else
    raise_application_error (-20001, 'Error in procedure parse_csv_by_field_name. File ' || p_csv_file || ' does not exist');
  end if;

exception when others then
    util.show_error ('Error in procedure parse_csv_by_field_name for file ID: ' || p_file_id, sqlerrm, true);
end parse_csv_by_field_name;

/******************************************************************************************************************************************************************/

--
-- load_session_info.
--
procedure load_session_info (p_directory in varchar2 default 'GARMIN')
is
l_column_list    varchar2 (2000);
l_file_list      varchar2 (2000);
begin
  set_nls;
  garmin_pkg.remove_csv_files;
  garmin_pkg.sync_fit_files;
  for ff in (select gff.id, gf.file_name, extract_filename (gf.file_name) fn from table (get_file_name (directory_path (p_directory), 'fit')) gf, gmn_fit_files gff
             where extract_filename (gf.file_name) = gff.file_name and gff.training is null)
  loop
	begin
      garmin_pkg.convert_fit_file_to_csv (ff.file_name, 'FitToCSV-session.bat');		
	  garmin_pkg.parse_csv_by_field_name (change_extention (ff.fn, 'csv'), ff.id, p_directory);
	  l_column_list  := 'insert into gmn_session_info (fit_id, ';
      l_file_list    := '(' || to_char (ff.id) || ', ';

      for j in (select distinct tc.column_name, tc.data_type, gc.unit, gc.val 
                from gmn_csv_by_field_name gc, user_tab_columns tc where tc.table_name = 'GMN_SESSION_INFO'  and upper (gc.field) = tc.column_name)
      loop
        l_column_list := l_column_list || j.column_name  || ', ';
        if    j.unit      = 'semicircles' then l_file_list := l_file_list || ' garmin_pkg.semicircles_to_lon_lat (' || j.val || '), ';
        elsif j.data_type = 'NUMBER'      then l_file_list := l_file_list || j.val || ', ';
        elsif j.data_type = 'DATE'        then l_file_list := l_file_list || ' garmin_pkg.date_offset_to_date (' || j.val || '), ';
        else l_file_list := l_file_list || '''' || j.val || ''', ';
        end if; 
      end loop;
      l_column_list := rtrim (rtrim (l_column_list), ',') || ') values ';
      l_file_list   := rtrim (rtrim (l_file_list)  , ',') || ')';  
      execute immediate l_column_list || l_file_list;
	  update gmn_fit_files set training = 1 where id = ff.id;
	  commit;
	  exception when others then null;
	  end;
  end loop;

exception when others then
    dbms_output.put_line (l_column_list);
	dbms_output.put_line (l_file_list);
    util.show_error ('Error in procedure load_session_info', sqlerrm, true);
end load_session_info;

/******************************************************************************************************************************************************************/

--
-- load_session_details
--
procedure load_session_details (p_directory in varchar2 default 'GARMIN')
is
l_statement   varchar2 (200);
l_prev_field  varchar2 (100) := '-1';
l_id          integer (6)    := 0;
l_user_id     gmn_devices.user_id%type;
begin
  set_nls;
  garmin_pkg.remove_csv_files;
    for ff in (select fn.file_name, extract_filename (fn.file_name) short_fn, gf.id fit_id, user_id from table (get_file_name (directory_path (p_directory), 'fit')) fn, gmn_fit_files gf
               where extract_filename (fn.file_name) = gf.file_name and gf.track is null)
    loop
        garmin_pkg.convert_fit_file_to_csv (ff.file_name, 'FitToCSV.bat');
		garmin_pkg.parse_csv_by_field_name (change_extention(ff.short_fn, 'csv'), ff.fit_id, p_directory);
		select user_id into l_user_id from gmn_devices where serial# = (select max(val) from gmn_csv_by_field_name where field =  'serial_number');
        l_id := 1;
        for j in (select gc.id, tc.column_name, gc.field, gc.val, gc.unit, tc.data_type
                  from gmn_csv_by_field_name gc, user_tab_columns tc
                  where tc.table_name = 'GMN_FIT_DATA' and upper (gc.field) = tc.column_name
                  order by gc.id)
        loop
          begin 
          if j.field = 'timestamp' and j.field != l_prev_field
          then
            l_id :=l_id + 1;
	        insert into gmn_fit_data (fit_id, id, person_id, "TIMESTAMP")  values (ff.fit_id, l_id, l_user_id, garmin_pkg.date_offset_to_date (j.val));  
          elsif    j.unit      = 'semicircles'
		  then l_statement := 'update gmn_fit_data set ' || j.column_name || '='||  garmin_pkg.semicircles_to_lon_lat (j.val) || 
                                                                ' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || l_user_id;
				execute immediate l_statement;
          elsif j.data_type = 'NUMBER'
		  then l_statement := 'update gmn_fit_data set ' || j.column_name || '='|| to_char (j.val) || 
                                                                ' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || l_user_id;
			    execute immediate l_statement;
          elsif j.data_type = 'DATE'
		  then  l_statement := 'update gmn_fit_data set ' || j.column_name || '='''|| garmin_pkg.date_offset_to_date (j.val) || 
                                                                ''' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || l_user_id;
				execute immediate l_statement;
          else l_statement := 'update gmn_fit_data set ' || j.column_name || '='''|| to_char (j.val) || 
                             ''' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || l_user_id;
			   execute immediate l_statement;
          end if;  

		update gmn_fit_files set track = 1, user_id = l_user_id where id = ff.fit_id;
		commit;
		
	  exception when others
	  then
	    dbms_output.put_line (l_statement);
		util.show_error ('Error in procedure load_session_details in inner loop', sqlerrm);
	  end;
	  l_prev_field := j.field;
	  end loop;
  end loop;
  delete gmn_fit_data where timestamp < to_date ('2000/01/01', 'YYYY/MM/DD');
  commit;

exception when others then 
   util.show_error ('Error in procedure load_session_details', sqlerrm);
end load_session_details;

/******************************************************************************************************************************************************************/

--
-- Remove_csv_files from directories that contain the .fit files
--
procedure remove_csv_files (p_directory in varchar2 default 'GARMIN')
is
begin
  utl_file.fclose_all;
  for f in (select extract_filename (file_name) filename from table (get_file_name (directory_path (p_directory), 'csv')))
  loop
	begin
      utl_file.fremove (p_directory, f.filename);
	exception when others
	then
	  begin
	    utl_file.frename (p_directory, f.filename, 'GARMIN_BACKUP', f.filename || '_' || to_char (sysdate, 'YYYYMonDD'), true);
	  exception when others then null;
	  end;
	end;
  end loop;

exception when others then 
   util.show_error ('Error in procedure remove_csv_files', sqlerrm);
end remove_csv_files;

/******************************************************************************************************************************************************************/

--
-- Returning corresponding range heartrate for a person: 1 = low, 2 = medium, 3 = high
--
function get_heartrate (p_person_id in integer, p_range in integer) return integer
is
l_heartrate number (3);
begin
  select case p_range when 1 then zone1 when 2 then zone2 when 3 then zone3 when 4 then zone4 when 5 then zone5  else null end
  into l_heartrate from gmn_heartrate_zones where person_id = p_person_id;
  return l_heartrate;

exception when no_data_found
then 
   util.show_error ('Error in function get_heartrate. Userid: ' || p_person_id || ' not present in table gmn_heartrate_zones.', sqlerrm);
   return null;
 when others then 
   util.show_error ('Error in function get_heartrate for userid: ' || p_person_id || ' and range: ' || p_range, sqlerrm);
   return null;
end get_heartrate;

/******************************************************************************************************************************************************************/

--
-- Checks if session details are present
--
function fit_data_loaded (p_fit_id in integer) return integer
is
l_ok integer (1);
begin
  select 1 into l_ok from gmn_fit_data where fit_id = p_fit_id and rownum = 1;
  return l_ok;

exception when no_data_found then return 0;
when others then 
  util.show_error ('Error in function fit_data_loaded for fit_id: ' || p_fit_id, sqlerrm);
  return null;
end fit_data_loaded;

/******************************************************************************************************************************************************************/

--
-- Reload fit data for a file
--
procedure reload_fit_data (p_fit_id in integer)
is
begin
  update gmn_fit_files set training = null, track = null where id = p_fit_id;
  delete gmn_session_info where fit_id = p_fit_id;
  delete gmn_fit_data     where fit_id = p_fit_id;
  garmin_pkg.load_session_info;
  garmin_pkg.load_session_details;

exception when others then 
   util.show_error ('Error in procedure reload_fit_data for ID: ' || p_fit_id, sqlerrm);
end reload_fit_data;

/******************************************************************************************************************************************************************/

--
-- Analytics pivot function
--
function analyze_data (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 40) return text_tab pipelined
is 
type cell_ty       is record (cell varchar2 (50));
type cell_row_ty   is table of cell_ty index by pls_integer;
type cell_field_ty is table of cell_row_ty index by pls_integer;
l_cells            cell_field_ty;
l_row              integer (4) := 0;
l_max_laps         integer (2) := 1;
begin 
  for y in 0 .. p_measurements
  loop
    l_cells (0) (y).cell := to_char (y);
    for x in 1 .. 50
    loop
      l_cells (x) (y).cell := null;
    end loop;
  end loop;
  l_cells (0) (0).cell := p_sport_profile;

  for d in (select ff.user_id, si.fit_id, si.sport_profile_name, trunc (si.start_time) start_time from gmn_session_info si, gmn_fit_files ff
            where ff.id = si.fit_id and si.sport_profile_name = p_sport_profile and ff.user_id = p_person_id
            order by si.start_time desc
            fetch first p_measurements rows only)
  loop
    l_row := l_row + 1;
    l_cells (l_row) (0).cell := to_char (d.start_time, 'YYYY/MM/DD');
    for j in (select rownum lap,avg_heart_rate, total_distance, round(3.6 *  enhanced_avg_speed, 2) enhanced_avg_speed, total_timer_time,
    case enhanced_avg_speed when 0 then null else substr (garmin_pkg.seconds_to_ds_interval (round(1000 / enhanced_avg_speed)), 15, 8) end tempo, total_calories, round (3.6 * enhanced_max_speed, 2) enhanced_max_speed, total_ascent, 
			  total_descent, enhanced_max_altitude
           from gmn_fit_data gf
           where gf.fit_id= d.fit_id and gf.start_position_lat is not null and gf.total_training_effect is null
           order by id)
    loop 
      case
	  when p_field = 1  then l_cells (l_row) (j.lap).cell := to_char (j.avg_heart_rate);
	  when p_field = 2  then l_cells (l_row) (j.lap).cell := to_char (round (j.total_distance));
	  when p_field = 3  then l_cells (l_row) (j.lap).cell := to_char (j.enhanced_avg_speed, '990D99');
	  when p_field = 4  then l_cells (l_row) (j.lap).cell := to_char (round (j.total_timer_time)); 
	  when p_field = 5  then l_cells (l_row) (j.lap).cell := to_char (j.tempo);
	  when p_field = 6  then l_cells (l_row) (j.lap).cell := to_char (j.total_calories); 
	  when p_field = 7  then l_cells (l_row) (j.lap).cell := to_char (to_char (j.enhanced_max_speed, '990D99')); 
	  when p_field = 8  then l_cells (l_row) (j.lap).cell := to_char (j.total_ascent); 
	  when p_field = 9  then l_cells (l_row) (j.lap).cell := to_char (j.total_descent);
	  when p_field = 10 then l_cells (l_row) (j.lap).cell := to_char (j.enhanced_max_altitude, '990D99');
      end case;
	  l_max_laps := greatest (l_max_laps, j.lap);
    end loop;
  end loop;

  for y in 0 .. l_max_laps
  loop
  begin
    pipe row (text_row (l_cells (0) (y).cell, l_cells (1) (y).cell, l_cells (2) (y).cell, l_cells (3) (y).cell, l_cells (4) (y).cell, l_cells (5) (y).cell,
              l_cells (6)  (y).cell, l_cells (7) (y).cell, l_cells   (8) (y).cell, l_cells  (9) (y).cell, l_cells (10) (y).cell,
              l_cells (11) (y).cell, l_cells (12) (y).cell, l_cells (13) (y).cell, l_cells (14) (y).cell, l_cells (15) (y).cell,
              l_cells (16) (y).cell, l_cells (17) (y).cell, l_cells (18) (y).cell, l_cells (19) (y).cell, l_cells (20) (y).cell));
    exception when others then null;
  end;
  end loop;

exception when others then 
   util.show_error ('Error in function analyze_data for person_id: ' || p_person_id || ',  Field: ' || p_field || '. Sport: ' || p_sport_profile, sqlerrm);
end analyze_data;

/******************************************************************************************************************************************************************/

--
-- Analytics function
--
function analyze_graph (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 20) return int_tab pipelined
is 
type cell_ty       is record (cell number (10, 2));
type cell_row_ty   is table of cell_ty     index by pls_integer;
type cell_field_ty is table of cell_row_ty index by pls_integer;
type train_date_ty is table of date        index by pls_integer;
l_cells            cell_field_ty;
l_row              integer (4) := 0;
l_train_dates      train_date_ty;
begin 
  for y in 1 .. p_measurements
  loop
  --  l_cells (0) (y).cell := y;
    for x in 1 .. 50
    loop
      l_cells (x) (y).cell := null;
    end loop;
  end loop;

  for d in (select ff.user_id, si.fit_id, si.sport_profile_name, trunc (si.start_time) start_time from gmn_session_info si, gmn_fit_files ff
            where ff.id = si.fit_id and si.sport_profile_name = p_sport_profile and ff.user_id = p_person_id
            order by si.start_time desc
            fetch first p_measurements rows only)
  loop
    l_row := l_row + 1;
    l_train_dates (l_row) := d.start_time;
    for j in (select rownum lap,avg_heart_rate, total_distance, round(3.6 *  enhanced_avg_speed, 2) enhanced_avg_speed, total_timer_time,
    case enhanced_avg_speed when 0 then null else round (1000 / enhanced_avg_speed) end tempo, total_calories, round (3.6 * enhanced_max_speed, 2) enhanced_max_speed, total_ascent, 
  			  total_descent, enhanced_max_altitude
              from gmn_fit_data gf
             where gf.fit_id= d.fit_id and gf.start_position_lat is not null and gf.total_training_effect is null
             order by id)
    loop
      case
  	  when p_field = 1  then l_cells (j.lap) (l_row).cell := j.avg_heart_rate;
  	  when p_field = 2  then l_cells (j.lap) (l_row).cell := round (j.total_distance);
  	  when p_field = 3  then l_cells (j.lap) (l_row).cell := round (j.enhanced_avg_speed, 2);
  	  when p_field = 4  then l_cells (j.lap) (l_row).cell := round (j.total_timer_time); 
  	  when p_field = 5  then l_cells (j.lap) (l_row).cell := j.tempo;
  	  when p_field = 6  then l_cells (j.lap) (l_row).cell := j.total_calories; 
  	  when p_field = 7  then l_cells (j.lap) (l_row).cell := round (j.enhanced_max_speed, 2); 
  	  when p_field = 8  then l_cells (j.lap) (l_row).cell := j.total_ascent; 
  	  when p_field = 9  then l_cells (j.lap) (l_row).cell := j.total_descent;
  	  when p_field = 10 then l_cells (j.lap) (l_row).cell := j.enhanced_max_altitude;
      end case;
    end loop;
  end loop;

  for y in 0 .. p_measurements
  loop
  begin
    pipe row (int_row (l_train_dates (y), l_cells (1) (y).cell, l_cells (2) (y).cell, l_cells (3) (y).cell, l_cells (4) (y).cell, l_cells (5) (y).cell,
              l_cells (6)  (y).cell, l_cells (7)  (y).cell, l_cells (8)  (y).cell, l_cells (9)  (y).cell, l_cells (10) (y).cell,
              l_cells (11) (y).cell, l_cells (12) (y).cell, l_cells (13) (y).cell, l_cells (14) (y).cell, l_cells (15) (y).cell,  
              l_cells (16) (y).cell, l_cells (17) (y).cell, l_cells (18) (y).cell, l_cells (19) (y).cell, l_cells (20) (y).cell));
  exception when others then null;
  end;
  end loop;

exception when others then 
   util.show_error ('Error in function analyze_graph for person_id: ' || p_person_id || ',  Field: ' || p_field || '. Sport: ' || p_sport_profile, sqlerrm);
end analyze_graph;

/******************************************************************************************************************************************************************/

--
-- Upload json files downloaded from Garmin in the database
--
procedure load_json_files (p_person_id in integer, p_directory in varchar2 default 'GARMIN')
is
l_bfile       bfile;
l_blob        blob := empty_blob;
l_dest_offset integer;
l_src_offset  integer;
begin
  for j in (select substr (file_name, 16) my_file from table (get_file_name (directory_path (p_directory), 'json'))
            where (substr (file_name, 16), p_person_id) not in (select name, person_id from gmn_json_data))
  loop
    if j.my_file like '%heartRateZones%' or j.my_file like '%sleepData%' or j.my_file like '%gear%'  or j.my_file like '%personalRecord%' or j.my_file like  '%userBioMetricProfileData%' or j.my_file like '%user_profile%' or 
       j.my_file like  '%UDSFile%' or j.my_file like  '%user_settings%' or j.my_file like  '%courses%' or j.my_file like  '%Predictions%' 
   then
	  l_dest_offset := 1;
      l_src_offset  := 1;
      l_bfile := bfilename (p_directory, j.my_file);
      dbms_lob.fileopen (l_bfile, dbms_lob.file_readonly);
      dbms_lob.createtemporary (l_blob, true);
      dbms_lob.open (l_blob, dbms_lob.lob_readwrite);
      dbms_lob.loadblobfromfile (
        dest_lob    => l_blob,
        src_bfile   => l_bfile,
        amount      => dbms_lob.lobmaxsize,
        dest_offset => l_dest_offset,
        src_offset  => l_src_offset);
      dbms_lob.fileclose (l_bfile);
      insert into gmn_json_data (name, person_id, json_clob) values (j.my_file, p_person_id, to_clob (l_blob));
	end if;
  end loop;
  commit;

exception when others then 
   util.show_error ('Error in procedure load_json_files for person_id: ' || p_person_id, sqlerrm);
end load_json_files;

/******************************************************************************************************************************************************************/

--
-- Move .fit files to the users backup directory
--
procedure archive_fit_files (p_retention_days in integer default 7)
is
begin
  for file in (select garmin_pkg.extract_filename (fn.file_name) short_fn, u.fit_directory
       from table (get_file_name (garmin_pkg.directory_path ('GARMIN'), 'fit')) fn, gmn_fit_files gf, gmn_users u
               where garmin_pkg.extract_filename (fn.file_name) = gf.file_name and gf.training = 1 and gf.track = 1
                 and fn.file_name like '%GARMIN\2%'
			     and u.id = gf.user_id and to_date (substr (gf.file_name, 1, 19), 'YYYY-MM-DD-HH24-MI-SS') < sysdate - p_retention_days)
  loop
    utl_file.frename ('GARMIN', file.short_fn, file.fit_directory, file.short_fn, true);
  end loop;

exception when others then 
   util.show_error ('Error in function archive_fit_files for retention: ' || p_retention_days, sqlerrm);
end archive_fit_files;

end garmin_pkg;
/









/* JSON views and tables */

--------------------------- Hydration

create or replace view v_gmn_json_hydration
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
hydration_calendarDate varchar2 (40) path '$.hydration.calendarDate',
hydration_capped varchar2 (10) path '$.hydration.capped',
hydration_goalInML number (8) path '$.hydration.goalInML',
hydration_lastEntryTimestampLocal varchar2 (40) path '$.hydration.lastEntryTimestampLocal',
hydration_sweatLossInML number (8) path '$.hydration.sweatLossInML',
hydration_userProfilePK number (16) path '$.hydration.userProfilePK',
hydration_valueInML number (8) path '$.hydration.valueInML',
includesActivityData varchar2 (10) path '$.includesActivityData',
includesAllDayPulseOx varchar2 (10) path '$.includesAllDayPulseOx',
includesCalorieConsumedData varchar2 (10) path '$.includesCalorieConsumedData',
includesContinuousMeasurement varchar2 (10) path '$.includesContinuousMeasurement',
includesSingleMeasurement varchar2 (10) path '$.includesSingleMeasurement',
includesSleepPulseOx varchar2 (10) path '$.includesSleepPulseOx',
includesWellnessData varchar2 (10) path '$.includesWellnessData',
latestSpo2Value number (2) path '$.latestSpo2Value',
latestSpo2ValueReadingTimeGmt varchar2 (40) path '$.latestSpo2ValueReadingTimeGmt',
latestSpo2ValueReadingTimeLocal varchar2 (40) path '$.latestSpo2ValueReadingTimeLocal',
lowestSpo2Value number (2) path '$.lowestSpo2Value',
maxAvgHeartRate number (4) path '$.maxAvgHeartRate',
maxHeartRate number (4) path '$.maxHeartRate',
minAvgHeartRate number (2) path '$.minAvgHeartRate',
minHeartRate number (2) path '$.minHeartRate',
moderateIntensityMinutes number (4) path '$.moderateIntensityMinutes',
remainingKilocalories number (8) path '$.remainingKilocalories',
respiration varchar2 (40) path '$.respiration',
respiration_avgWakingRespirationValue number (4) path '$.respiration.avgWakingRespirationValue',
respiration_calendarDate varchar2 (40) path '$.respiration.calendarDate',
respiration_highestRespirationValue number (4) path '$.respiration.highestRespirationValue',
respiration_latestRespirationTimeGMT varchar2 (40) path '$.respiration.latestRespirationTimeGMT',
respiration_latestRespirationValue number (4) path '$.respiration.latestRespirationValue',
respiration_lowestRespirationValue number (4) path '$.respiration.lowestRespirationValue',
respiration_userProfilePK number (16) path '$.respiration.userProfilePK',
restingCaloriesFromActivity number (8) path '$.restingCaloriesFromActivity',
restingHeartRate number (2) path '$.restingHeartRate',
restingHeartRateTimestamp number (16) path '$.restingHeartRateTimestamp',
source number (1) path '$.source',
totalDistanceMeters number (8) path '$.totalDistanceMeters',
totalKilocalories number (8) path '$.totalKilocalories',
totalSteps number (8) path '$.totalSteps',
userFloorsAscendedGoal number (2) path '$.userFloorsAscendedGoal',
userIntensityMinutesGoal number (4) path '$.userIntensityMinutesGoal',
userProfilePK number (16) path '$.userProfilePK',
uuid varchar2 (40) path '$.uuid',
version number (8) path '$.version',
vigorousIntensityMinutes number (4) path '$.vigorousIntensityMinutes',
wellnessActiveKilocalories number (8) path '$.wellnessActiveKilocalories',
wellnessDistanceMeters number (8) path '$.wellnessDistanceMeters',
wellnessEndTimeGmt varchar2 (40) path '$.wellnessEndTimeGmt',
wellnessEndTimeLocal varchar2 (40) path '$.wellnessEndTimeLocal',
wellnessKilocalories number (8) path '$.wellnessKilocalories',
wellnessStartTimeGmt varchar2 (40) path '$.wellnessStartTimeGmt',
wellnessStartTimeLocal varchar2 (40) path '$.wellnessStartTimeLocal',
wellnessTotalKilocalories number (8) path '$.wellnessTotalKilocalories') jt
where hydration_calendarDate is not null and jd.name like  '%UDSFile%'
order by hydration_calendarDate;

--------------------------- Biometrics profile

create or replace view v_gmn_json_biometrics_profile
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
  activityClass number (1) path '$.activityClass',
  functionalThresholdPower number (4) path '$.functionalThresholdPower',
  height number (8) path '$.height',
  vo2Max number (4) path '$.vo2Max',
  vo2MaxCycling number (4) path '$.vo2MaxCycling',
  weight number (8) path '$.weight') jt
where activityClass is not null and jd.name like  '%userBioMetricProfileData%';

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
select activityclass, functionalthresholdpower, height, vo2max, vo2maxcycling, weight, person_id from v_gmn_json_biometrics_profile
where (activityclass, functionalthresholdpower, height, vo2max, vo2maxcycling, weight, person_id)
not in (select activity_class, threshold_power, height, vo2_max, vo2_max_cycling, weight, person_id from gmn_biometic_profile);

--------------------------- Courses

create or replace view v_gmn_json_courses
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
accessControl varchar2 (40) path '$.accessControl',
courseName varchar2 (40) path '$.courseName',
courseType varchar2 (40) path '$.courseType',
createDate varchar2 (40) path '$.createDate',
elevationGainMeter number (8) path '$.elevationGainMeter',
elevationLossMeter number (8) path '$.elevationLossMeter',
hasTurnDetectionDisabled varchar2 (10) path '$.hasTurnDetectionDisabled',
startPoint_elevation number (32) path '$.startPoint.elevation',
startPoint_latitude number (32) path '$.startPoint.latitude',
startPoint_longitude number (16) path '$.startPoint.longitude',
updateDate varchar2 (40) path '$.updateDate') jt
where accessControl is not null and jd.name like  '%courses%'
order by createDate;

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
and (coursename,person_id, createdate) in (select coursename,person_id, max(createdate) from v_gmn_json_courses  group by coursename,person_id);

--------------------------- Gear

-- Gear
create or replace view v_gmn_json_gear
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$.gearDTOS[*]'
         columns
createDate varchar2 (40) path '$.createDate',
customMakeModel varchar2 (40) path '$.customMakeModel',
dateBegin varchar2 (40) path '$.dateBegin',
displayName varchar2 (40) path '$.displayName',
gearPk number (8) path '$.gearPk',
gearStatusName varchar2 (40) path '$.gearStatusName',
maximumMeters number (4) path '$.maximumMeters',
notified varchar2 (10) path '$.notified',
updateDate varchar2 (40) path '$.updateDate',
userProfilePk number (16) path '$.userProfilePk',
uuid varchar2 (40) path '$.uuid') jt
where jd.name like '%gear%';

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
select gearpk, to_date (createdate, 'YYYY-MM-DD'), custommakemodel, to_date (datebegin, 'YYYY-MM-DD'), displayname, gearstatusname,
 maximummeters, notified, to_date(updatedate, 'YYYY-MM-DD'), person_id
from v_gmn_json_gear;

--------------------------- Heartrates xones

create or replace view v_gmn_json_heartrate_zones
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
changeState varchar2 (40) path '$.changeState',
maxHeartRateUsed number (4) path '$.maxHeartRateUsed',
restingHrAutoUpdateUsed varchar2 (10) path '$.restingHrAutoUpdateUsed',
sport varchar2 (40) path '$.sport',
trainingMethod varchar2 (40) path '$.trainingMethod',
zone1Floor number (2) path '$.zone1Floor',
zone2Floor number (4) path '$.zone2Floor',
zone3Floor number (4) path '$.zone3Floor',
zone4Floor number (4) path '$.zone4Floor',
zone5Floor number (4) path '$.zone5Floor') jt
where name like '%heartRateZones%' and changestate is not null;

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

insert into gmn_heartrate_zones (person_id, sport, trainingmethod, zone1, zone2, zone3, zone4, zone5)        
select person_id, sport, trainingmethod, zone1floor, zone2floor, zone3floor, zone4floor, zone5floor from v_gmn_json_heartrate_zones;

--------------------------- Personal records

create or replace view v_gmn_json_personal_records
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$.personalRecords[*]'
         columns
activityId number (16) path '$.activityId',
confirmed varchar2 (10) path '$.confirmed',
createdDate varchar2 (40) path '$.createdDate',
current_v varchar2 (10) path '$.current',
personalRecordId number (16) path '$.personalRecordId',
personalRecordType varchar2 (40) path '$.personalRecordType',
prStartTimeGMT varchar2 (40) path '$.prStartTimeGMT',
value number (32) path '$.value') jt
where activityId is not null and jd.name like '%personalRecord%'
order by createdDate;

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

--------------------------- Runrace predictions

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

--------------------------- User profile

create or replace view v_gmn_json_user_profile
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
birthDate varchar2 (40) path '$.birthDate',
emailAddress varchar2 (40) path '$.emailAddress',
firstName varchar2 (40) path '$.firstName',
gender varchar2 (40) path '$.gender',
lastName varchar2 (40) path '$.lastName',
userName varchar2 (40) path '$.userName') jt
where birthDate is not null and jd.name like  '%user_profile%';
 
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

--------------------------- User settings

create or replace view v_gmn_json_user_settings
as 
select distinct jt.*, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
handedness varchar2 (40) path '$.handedness',
preferredLocale varchar2 (40) path '$.preferredLocale') jt
where handedness is not null and jd.name like  '%user_settings%';

create table gmn_user_settings
( person_id     number (4)
, handedness    varchar2 (10)
, locale        varchar2 (10));

insert into gmn_user_settings ( person_id, handedness, locale)
select person_id, handedness, preferredlocale from v_gmn_json_user_settings;

alter table gmn_user_settings add constraint gmn_user_settings_pk primary key (person_id) using index;

--------------------------- Sleep data

-- Sleep data
create or replace view v_gmn_json_sleep_data
as 
select distinct jt.*, to_date(jt.CalendarDate, 'YYYY-MM-DD') cal_date, 
to_date(substr(sleependtimestampgmt, 1, 10) || substr(sleependtimestampgmt, 12, 8), 'YYYY-MM-DDHH24:MI:SS') sleep_end,
to_date(substr(sleepstarttimestampgmt, 1, 10) || substr(sleepstarttimestampgmt, 12, 8), 'YYYY-MM-DDHH24:MI:SS') sleep_start, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
averageRespiration number (4) path '$.averageRespiration',
avgSleepStress number (32) path '$.avgSleepStress',
awakeCount number (2) path '$.awakeCount',
awakeSleepSeconds number (8) path '$.awakeSleepSeconds',
calendarDate varchar2 (40) path '$.calendarDate',
deepSleepSeconds number (4) path '$.deepSleepSeconds',
highestRespiration number (4) path '$.highestRespiration',
lightSleepSeconds number (8) path '$.lightSleepSeconds',
lowestRespiration number (4) path '$.lowestRespiration',
remSleepSeconds number (4) path '$.remSleepSeconds',
restlessMomentCount number (4) path '$.restlessMomentCount',
retro varchar2 (10) path '$.retro',
sleepEndTimestampGMT varchar2 (40) path '$.sleepEndTimestampGMT',
-- sleepScores varchar2 (40) path '$.sleepScores',
sleepScores_awakeTimeScore number (2) path '$.sleepScores.awakeTimeScore',
sleepScores_awakeningsCountScore number (2) path '$.sleepScores.awakeningsCountScore',
sleepScores_combinedAwakeScore number (2) path '$.sleepScores.combinedAwakeScore',
sleepScores_deepScore number (2) path '$.sleepScores.deepScore',
sleepScores_durationScore number (4) path '$.sleepScores.durationScore',
sleepScores_feedback varchar2 (40) path '$.sleepScores.feedback',
sleepScores_insight varchar2 (40) path '$.sleepScores.insight',
sleepScores_interruptionsScore number (2) path '$.sleepScores.interruptionsScore',
sleepScores_lightScore number (2) path '$.sleepScores.lightScore',
sleepScores_overallScore number (2) path '$.sleepScores.overallScore',
sleepScores_qualityScore number (2) path '$.sleepScores.qualityScore',
sleepScores_recoveryScore number (4) path '$.sleepScores.recoveryScore',
sleepScores_remScore number (4) path '$.sleepScores.remScore',
sleepScores_restfulnessScore number (2) path '$.sleepScores_restfulnessScore',
sleepStartTimestampGMT varchar2 (40) path '$.sleepStartTimestampGMT',
sleepWindowConfirmationType varchar2 (40) path '$.sleepWindowConfirmationType',
unmeasurableSeconds number (1) path '$.unmeasurableSeconds') jt
where  name like '%sleepData%' and avgSleepStress is not null or averageRespiration is not null or awakeCount is not null
order by calendardate;


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

--------------------------- ToDo

 
 V_GMN_JSON_TRAINING_READYNESS
 
 V_GMN_SESSION_INFO_STEP_LENGTH
 
 V_GMN_JSON_HYDRATION
 
 
create table gmn_logbook
( id         integer generated always as identity
, user_id    number (6) not null
, created    date
, updated    date
, picture    blob 
, text       varchar2 (2000));

alter table gmn_logbook add constraint gmn_logbook_pk primary key (id) using index;
alter table gmn_logbook add constraint gmn_logbook_fk1 foreign key (user_id) references gmn_users (id) on delete cascade;

create or replace trigger gmn_logbook_briu
before insert or update on gmn_logbook
for each row 
begin 
  if inserting
  then :new.created := sysdate;
  else :new.updated := sysdate;
  end if;
  if :new.picture is null
  then select u.avatar into :new.picture from gmn_users u where u.id = :new.user_id;
  end if;
end gmn_logbook_briu;
/
  
Add serial# and time offset.

https://www.youtube.com/watch?v=JzDsvuEBRaU

Source code
https://drive.google.com/drive/folders/1fpcG9lA2Wy9axb4arsDI1TDDEyQoGOPZ