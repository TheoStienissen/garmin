DOC

  Author      :  Theo Stienissen
  Date        :  October  2023
  Last Change :  July     2024
  Purpose     :  Analyze data from Garmin watch in an Oracle database
  Status      :  Production Acceptance
  Contact     :  theo.stienissen@gmail.com
  @C:\Users\Theo\OneDrive\Theo\Project\garmin\garmin7.sql
  Fit Protocol https://developer.garmin.com/fit/protocol/

Garmin data export: https://www.garmin.com/en-GB/account/datamanagement/exportdata/

There are 3 possible data sources to this solution:
1. Downloaded CSV from the activities page: activities.csv This data is loaded in the GARMIN table.
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

ToDo:
Drop the columns from gmn_users:
, hr_low        number (3)    not null default 100
, hr_medium     number (3)    not null default 120
, hr_high       number (3)    not null default 140

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
, avatar        blob          not null      
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

insert into gmn_devices (user_id, description, serial#) values (1, 'Garmin watch Theo', 3426042514);
insert into gmn_devices (user_id, description, serial#) values (2, 'Garmin watch Dolly', 3446481735);

create table gmn_fit_routines
( id      integer generated always as identity
, path    varchar2 (100)
, routine varchar2 (50)
, version varchar2 (20));

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

create global temporary table gmn_csv_by_field_name
( line_number      number (6)
, column_position  number (3)
, field            varchar2 (50)
, val              varchar2 (150)
, unit             varchar2 (50))
on commit delete rows;

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
local_timestamp                  date,
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

create or replace trigger gmn_session_info_bri
before insert on gmn_session_info
for each row
begin
for  j in (select to_date (to_char (year) || '03' || to_char (dst_start) || '03', 'YYYYMMDDHH24') dst_start, to_date (to_char (year) || '10' || to_char (dst_end) || '03', 'YYYYMMDDHH24') dst_end
           from gmn_dst_data
		   where year  = to_number (to_char (:new.start_time, 'YYYY')))
loop
  if :new.local_timestamp is null
  then
    if :new.start_time between j.dst_start and j.dst_end
    then :new.local_timestamp := :new.start_time + 2 / 24; -- Summertime 
    else :new.local_timestamp := :new.start_time + 1 / 24; -- Wintertime 
  end if;
  end if;
end loop;
end gmn_session_info_bri;
/

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
, local_timestamp      date,   -- start time
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
( id          integer generated always as identity
, person_id   number (6) not null
, name        varchar2 (100)
, json_clob   clob
, loaded      date default sysdate
, view_name   varchar2 (50));

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


create global temporary table gmn_excel_output
( line_number  number (10),
col001 varchar2 (200),col002 varchar2 (200),col003 varchar2 (200),col004 varchar2 (200),col005 varchar2 (200),col006 varchar2 (200),col007 varchar2 (200),
col008 varchar2 (200),col009 varchar2 (200),col010 varchar2 (200),col011 varchar2 (200),col012 varchar2 (200),col013 varchar2 (200),col014 varchar2 (200),
col015 varchar2 (200),col016 varchar2 (200),col017 varchar2 (200),col018 varchar2 (200),col019 varchar2 (200),col020 varchar2 (200),col021 varchar2 (200),
col022 varchar2 (200),col023 varchar2 (200),col024 varchar2 (200),col025 varchar2 (200),col026 varchar2 (200),col027 varchar2 (200),col028 varchar2 (200),
col029 varchar2 (200),col030 varchar2 (200),col031 varchar2 (200),col032 varchar2 (200),col033 varchar2 (200),col034 varchar2 (200),col035 varchar2 (200),
col036 varchar2 (200),col037 varchar2 (200),col038 varchar2 (200),col039 varchar2 (200),col040 varchar2 (200),col041 varchar2 (200),col042 varchar2 (200),
col043 varchar2 (200),col044 varchar2 (200),col045 varchar2 (200),col046 varchar2 (200),col047 varchar2 (200),col048 varchar2 (200),col049 varchar2 (200),
col050 varchar2 (200),col051 varchar2 (200),col052 varchar2 (200),col053 varchar2 (200),col054 varchar2 (200),col055 varchar2 (200),col056 varchar2 (200),
col057 varchar2 (200),col058 varchar2 (200),col059 varchar2 (200),col060 varchar2 (200),col061 varchar2 (200),col062 varchar2 (200),col063 varchar2 (200),
col064 varchar2 (200),col065 varchar2 (200),col066 varchar2 (200),col067 varchar2 (200),col068 varchar2 (200),col069 varchar2 (200),col070 varchar2 (200),
col071 varchar2 (200),col072 varchar2 (200),col073 varchar2 (200),col074 varchar2 (200),col075 varchar2 (200),col076 varchar2 (200),col077 varchar2 (200),
col078 varchar2 (200),col079 varchar2 (200),col080 varchar2 (200),col081 varchar2 (200),col082 varchar2 (200),col083 varchar2 (200),col084 varchar2 (200),
col085 varchar2 (200),col086 varchar2 (200),col087 varchar2 (200),col088 varchar2 (200),col089 varchar2 (200),col090 varchar2 (200),col091 varchar2 (200),
col092 varchar2 (200),col093 varchar2 (200),col094 varchar2 (200),col095 varchar2 (200),col096 varchar2 (200),col097 varchar2 (200),col098 varchar2 (200),
col099 varchar2 (200),col100 varchar2 (200),col101 varchar2 (200),col102 varchar2 (200),col103 varchar2 (200),col104 varchar2 (200),col105 varchar2 (200),
col106 varchar2 (200),col107 varchar2 (200),col108 varchar2 (200),col109 varchar2 (200),col110 varchar2 (200),col111 varchar2 (200),col112 varchar2 (200),
col113 varchar2 (200),col114 varchar2 (200),col115 varchar2 (200),col116 varchar2 (200),col117 varchar2 (200),col118 varchar2 (200),col119 varchar2 (200),
col120 varchar2 (200),col121 varchar2 (200),col122 varchar2 (200),col123 varchar2 (200),col124 varchar2 (200),col125 varchar2 (200),col126 varchar2 (200),
col127 varchar2 (200),col128 varchar2 (200),col129 varchar2 (200),col130 varchar2 (200),col131 varchar2 (200),col132 varchar2 (200),col133 varchar2 (200),
col134 varchar2 (200),col135 varchar2 (200),col136 varchar2 (200),col137 varchar2 (200),col138 varchar2 (200),col139 varchar2 (200),col140 varchar2 (200),
col141 varchar2 (200),col142 varchar2 (200),col143 varchar2 (200),col144 varchar2 (200),col145 varchar2 (200),col146 varchar2 (200),col147 varchar2 (200),
col148 varchar2 (200),col149 varchar2 (200),col150 varchar2 (200),col151 varchar2 (200),col152 varchar2 (200),col153 varchar2 (200),col154 varchar2 (200),
col155 varchar2 (200),col156 varchar2 (200),col157 varchar2 (200),col158 varchar2 (200),col159 varchar2 (200),col160 varchar2 (200),col161 varchar2 (200),
col162 varchar2 (200),col163 varchar2 (200),col164 varchar2 (200),col165 varchar2 (200),col166 varchar2 (200),col167 varchar2 (200),col168 varchar2 (200),
col169 varchar2 (200),col170 varchar2 (200),col171 varchar2 (200),col172 varchar2 (200),col173 varchar2 (200),col174 varchar2 (200),col175 varchar2 (200),
col176 varchar2 (200),col177 varchar2 (200),col178 varchar2 (200),col179 varchar2 (200),col180 varchar2 (200),col181 varchar2 (200),col182 varchar2 (200),
col183 varchar2 (200),col184 varchar2 (200),col185 varchar2 (200),col186 varchar2 (200),col187 varchar2 (200),col188 varchar2 (200),col189 varchar2 (200),
col190 varchar2 (200),col191 varchar2 (200),col192 varchar2 (200),col193 varchar2 (200),col194 varchar2 (200),col195 varchar2 (200),col196 varchar2 (200),
col197 varchar2 (200),col198 varchar2 (200),col199 varchar2 (200),col200 varchar2 (200),col201 varchar2 (200),col202 varchar2 (200),col203 varchar2 (200),
col204 varchar2 (200),col205 varchar2 (200),col206 varchar2 (200),col207 varchar2 (200),col208 varchar2 (200),col209 varchar2 (200),col210 varchar2 (200),
col211 varchar2 (200),col212 varchar2 (200),col213 varchar2 (200),col214 varchar2 (200),col215 varchar2 (200),col216 varchar2 (200),col217 varchar2 (200),
col218 varchar2 (200),col219 varchar2 (200),col220 varchar2 (200),col221 varchar2 (200),col222 varchar2 (200),col223 varchar2 (200),col224 varchar2 (200),
col225 varchar2 (200),col226 varchar2 (200),col227 varchar2 (200),col228 varchar2 (200),col229 varchar2 (200),col230 varchar2 (200),col231 varchar2 (200),
col232 varchar2 (200),col233 varchar2 (200),col234 varchar2 (200),col235 varchar2 (200),col236 varchar2 (200),col237 varchar2 (200),col238 varchar2 (200),
col239 varchar2 (200),col240 varchar2 (200),col241 varchar2 (200),col242 varchar2 (200),col243 varchar2 (200),col244 varchar2 (200),col245 varchar2 (200),
col246 varchar2 (200),col247 varchar2 (200),col248 varchar2 (200),col249 varchar2 (200),col250 varchar2 (200),col251 varchar2 (200),col252 varchar2 (200),
col253 varchar2 (200),col254 varchar2 (200),col255 varchar2 (200),col256 varchar2 (200),col257 varchar2 (200),col258 varchar2 (200),col259 varchar2 (200),
col260 varchar2 (200),col261 varchar2 (200),col262 varchar2 (200),col263 varchar2 (200),col264 varchar2 (200),col265 varchar2 (200),col266 varchar2 (200),
col267 varchar2 (200),col268 varchar2 (200),col269 varchar2 (200),col270 varchar2 (200),col271 varchar2 (200),col272 varchar2 (200),col273 varchar2 (200),
col274 varchar2 (200),col275 varchar2 (200),col276 varchar2 (200),col277 varchar2 (200),col278 varchar2 (200),col279 varchar2 (200),col280 varchar2 (200),
col281 varchar2 (200),col282 varchar2 (200),col283 varchar2 (200),col284 varchar2 (200),col285 varchar2 (200),col286 varchar2 (200),col287 varchar2 (200),
col288 varchar2 (200),col289 varchar2 (200),col290 varchar2 (200),col291 varchar2 (200),col292 varchar2 (200),col293 varchar2 (200),col294 varchar2 (200),
col295 varchar2 (200),col296 varchar2 (200),col297 varchar2 (200),col298 varchar2 (200))
on commit delete rows;


set serveroutput on size unlimited

create or replace package garmin_pkg
is
g_max_int        constant integer     := power (2, 31);

function  slash return varchar2;

function  extract_filename (p_filename in varchar2) return varchar2;

function  change_extention (p_filename in varchar2, p_new_extention in varchar2) return varchar2;

function  directory_path (p_directory in varchar2 default 'GARMIN') return varchar2;

procedure set_nls;

function  file_to_blob (p_dir in varchar2, p_filename in varchar2) return blob;

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

procedure parse_csv_by_field_name (p_csv_file in varchar2, p_directory in varchar2 default 'GARMIN', p_skip integer default 1);

procedure load_session_info (p_directory in varchar2 default 'GARMIN');

procedure load_session_details (p_directory in varchar2 default 'GARMIN');

procedure remove_csv_files (p_directory in varchar2 default 'GARMIN');

function  get_heartrate (p_person_id in integer, p_range in integer) return integer;

function  fit_data_loaded (p_fit_id in integer) return integer;

procedure reload_fit_data (p_fit_id in integer);

function  analyze_data (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 40) return text_tab pipelined;

function  analyze_graph (p_person_id in integer, p_field in integer, p_sport_profile in varchar2, p_measurements in integer default 20) return int_tab pipelined;

procedure load_json_files (p_person_id in integer, p_directory in varchar2 default 'GARMIN');

procedure evaluate_json_data;

procedure archive_fit_files (p_retention_days in integer default 7);

end garmin_pkg;
/


create or replace package body garmin_pkg
is
function slash return varchar2
is
begin
  if dbms_utility.port_string like '%WIN%'
  then return chr (92);  -- Windows
  else return chr (47);  -- Linux
  end if;

exception when others then
  util.show_error ('Error in function function slash', sqlerrm);
  return null;
end slash;
 
/******************************************************************************************************************************************************************/

--
-- Strip filename from a path / string. Cut after the last slash or backslash
--
function extract_filename (p_filename in varchar2) return varchar2
is
l_filename varchar2 (200) := p_filename;
begin
  select substr (p_filename, - instr (reverse (p_filename), slash) + 1) into l_filename from dual;
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
  select case when directory_path like '%' || slash then directory_path else directory_path || slash end into l_directory_path from dba_directories where directory_name = upper (p_directory);
  return l_directory_path;  

exception when others then
  util.show_error ('Error in function directory_path for directory: ' || p_directory, sqlerrm);
  return null;
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
-- Load a file from disk and return it as a blob
--
function  file_to_blob (p_dir in varchar2, p_filename in varchar2) return blob
is
  l_bfile  bfile;
  l_blob   blob;
  l_dest_offset integer := 1;
  l_src_offset  integer := 1;
begin
  l_bfile := bfilename (p_dir, p_filename);
  dbms_lob.fileopen (l_bfile, dbms_lob.file_readonly);
  dbms_lob.createtemporary(l_blob, false);
  if dbms_lob.getlength(l_bfile) > 0 then
    dbms_lob.loadblobfromfile (
      dest_lob    => l_blob,
      src_bfile   => l_bfile,
      amount      => dbms_lob.lobmaxsize,
      dest_offset => l_dest_offset,
      src_offset  => l_src_offset);
  end if;
  dbms_lob.fileclose(l_bfile);
  return l_blob;
  
exception when others then
  util.show_error ('Error in function  file_to_blob for file: ' || p_filename || ' in directory: ' || p_dir, sqlerrm);
end file_to_blob;

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
  elsif instr (p_string, ':') = 0            then l_ds := to_dsinterval ( '00 00:00:' || replace (p_string, ',', '.'));
  elsif instr (p_string, ':', 1, 2) = 0      then l_ds := to_dsinterval ( '00 00:'    || replace (p_string, ',', '.'));
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
  if   instr (p_string, ':', 1, 2) > 0
  then
    if instr (p_string, '-') > 0
    then l_date := to_date (p_string, 'YYYY-MM-DD HH24:MI:SS');
	else l_date := to_date (p_string, 'MM/DD/YYYY HH24:MI:SS');
	end if;
  else
    if instr (p_string, '-') > 0
    then l_date := to_date (p_string, 'YYYY-MM-DD HH24:MI');
    else l_date := to_date (p_string, 'MM/DD/YYYY HH24:MI');
    end if;
  end if;
  return l_date;

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
  return to_date ('31-12-1989', 'DD-MM-YYYY') + p_offset / 86400;

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
	   where (instr(file_name, slash, 1, 4) = 0)
	     and  garmin_pkg.extract_filename (file_name) not in (select file_name from gmn_fit_files) order by 1 desc;		
  commit;

exception when others then 
   util.show_error ('Error in procedure sync_fit_files for directory: ' || p_directory, sqlerrm);
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
l_action      varchar2 (200);
begin
   select path || p_routine into l_action from gmn_fit_routines where routine = p_routine;
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
procedure parse_csv_by_field_name (p_csv_file in varchar2, p_directory in varchar2 default 'GARMIN', p_skip integer default 1)
is
begin
  
  insert into gmn_excel_output (
  line_number, col001,col002,col003,col004,col005,col006,col007,col008,col009,col010,col011,col012,col013,col014,col015,col016,
  col017,col018,col019,col020,col021,col022,col023,col024,col025,col026,col027,col028,col029,col030,col031,col032,col033,col034,col035,
  col036,col037,col038,col039,col040,col041,col042,col043,col044,col045,col046,col047,col048,col049,col050,col051,col052,col053,col054,
  col055,col056,col057,col058,col059,col060,col061,col062,col063,col064,col065,col066,col067,col068,col069,col070,col071,col072,col073,
  col074,col075,col076,col077,col078,col079,col080,col081,col082,col083,col084,col085,col086,col087,col088,col089,col090,col091,col092,
  col093,col094,col095,col096,col097,col098,col099,col100,col101,col102,col103,col104,col105,col106,col107,col108,col109,col110,col111,
  col112,col113,col114,col115,col116,col117,col118,col119,col120,col121,col122,col123,col124,col125,col126,col127,col128,col129,col130,
  col131,col132,col133,col134,col135,col136,col137,col138,col139,col140,col141,col142,col143,col144,col145,col146,col147,col148,col149,
  col150,col151,col152,col153,col154,col155,col156,col157,col158,col159,col160,col161,col162,col163,col164,col165,col166,col167,col168,
  col169,col170,col171,col172,col173,col174,col175,col176,col177,col178,col179,col180,col181,col182,col183,col184,col185,col186,col187,
  col188,col189,col190,col191,col192,col193,col194,col195,col196,col197,col198,col199,col200,col201,col202,col203,col204,col205,col206,
  col207,col208,col209,col210,col211,col212,col213,col214,col215,col216,col217,col218,col219,col220,col221,col222,col223,col224,col225,
  col226,col227,col228,col229,col230,col231,col232,col233,col234,col235,col236,col237,col238,col239,col240,col241,col242,col243,col244,
  col245,col246,col247,col248,col249,col250,col251,col252,col253,col254,col255,col256,col257,col258,col259,col260,col261,col262,col263,
  col264,col265,col266,col267,col268,col269,col270,col271,col272,col273,col274,col275,col276,col277,col278,col279,col280,col281,col282,
  col283,col284,col285,col286,col287,col288,col289,col290,col291,col292,col293,col294,col295,col296,col297)
  select line_number, col001,col002,col003,col004,col005,col006,col007,col008,col009,col010,col011,col012,col013,col014,col015,col016,
  col017,col018,col019,col020,col021,col022,col023,col024,col025,col026,col027,col028,col029,col030,col031,col032,col033,col034,col035,
  col036,col037,col038,col039,col040,col041,col042,col043,col044,col045,col046,col047,col048,col049,col050,col051,col052,col053,col054,
  col055,col056,col057,col058,col059,col060,col061,col062,col063,col064,col065,col066,col067,col068,col069,col070,col071,col072,col073,
  col074,col075,col076,col077,col078,col079,col080,col081,col082,col083,col084,col085,col086,col087,col088,col089,col090,col091,col092,
  col093,col094,col095,col096,col097,col098,col099,col100,col101,col102,col103,col104,col105,col106,col107,col108,col109,col110,col111,
  col112,col113,col114,col115,col116,col117,col118,col119,col120,col121,col122,col123,col124,col125,col126,col127,col128,col129,col130,
  col131,col132,col133,col134,col135,col136,col137,col138,col139,col140,col141,col142,col143,col144,col145,col146,col147,col148,col149,
  col150,col151,col152,col153,col154,col155,col156,col157,col158,col159,col160,col161,col162,col163,col164,col165,col166,col167,col168,
  col169,col170,col171,col172,col173,col174,col175,col176,col177,col178,col179,col180,col181,col182,col183,col184,col185,col186,col187,
  col188,col189,col190,col191,col192,col193,col194,col195,col196,col197,col198,col199,col200,col201,col202,col203,col204,col205,col206,
  col207,col208,col209,col210,col211,col212,col213,col214,col215,col216,col217,col218,col219,col220,col221,col222,col223,col224,col225,
  col226,col227,col228,col229,col230,col231,col232,col233,col234,col235,col236,col237,col238,col239,col240,col241,col242,col243,col244,
  col245,col246,col247,col248,col249,col250,col251,col252,col253,col254,col255,col256,col257,col258,col259,col260,col261,col262,col263,
  col264,col265,col266,col267,col268,col269,col270,col271,col272,col273,col274,col275,col276,col277,col278,col279,col280,col281,col282,
  col283,col284,col285,col286,col287,col288,col289,col290,col291,col292,col293,col294,col295,col296,col297
  from table (apex_data_parser.parse (p_content => file_to_blob (p_directory, p_csv_file), p_file_name =>  p_csv_file, p_skip_rows => p_skip))
  where col001 = 'Data' and col003 != 'unknown';
   
  insert into gmn_csv_by_field_name (line_number, column_position, field, val, unit)
  select line_number,   4, col004, col005, col006 from gmn_excel_output where col004 is not null and col004 != 'unknown' union all 
  select line_number,   7, col007, col008, col009 from gmn_excel_output where col007 is not null and col007 != 'unknown' union all 
  select line_number,  10, col010, col011, col012 from gmn_excel_output where col010 is not null and col010 != 'unknown' union all 
  select line_number,  13, col013, col014, col015 from gmn_excel_output where col013 is not null and col013 != 'unknown' union all 
  select line_number,  16, col016, col017, col018 from gmn_excel_output where col016 is not null and col016 != 'unknown' union all 
  select line_number,  19, col019, col020, col021 from gmn_excel_output where col019 is not null and col019 != 'unknown' union all 
  select line_number,  22, col022, col023, col024 from gmn_excel_output where col022 is not null and col022 != 'unknown' union all 
  select line_number,  25, col025, col026, col027 from gmn_excel_output where col025 is not null and col025 != 'unknown' union all 
  select line_number,  28, col028, col029, col030 from gmn_excel_output where col028 is not null and col028 != 'unknown' union all 
  select line_number,  31, col031, col032, col033 from gmn_excel_output where col031 is not null and col031 != 'unknown' union all 
  select line_number,  34, col034, col035, col036 from gmn_excel_output where col034 is not null and col034 != 'unknown' union all 
  select line_number,  37, col037, col038, col039 from gmn_excel_output where col037 is not null and col037 != 'unknown' union all 
  select line_number,  40, col040, col041, col042 from gmn_excel_output where col040 is not null and col040 != 'unknown' union all 
  select line_number,  43, col043, col044, col045 from gmn_excel_output where col043 is not null and col043 != 'unknown' union all 
  select line_number,  46, col046, col047, col048 from gmn_excel_output where col046 is not null and col046 != 'unknown' union all 
  select line_number,  49, col049, col050, col051 from gmn_excel_output where col049 is not null and col049 != 'unknown' union all 
  select line_number,  52, col052, col053, col054 from gmn_excel_output where col052 is not null and col052 != 'unknown' union all 
  select line_number,  55, col055, col056, col057 from gmn_excel_output where col055 is not null and col055 != 'unknown' union all 
  select line_number,  58, col058, col059, col060 from gmn_excel_output where col058 is not null and col058 != 'unknown' union all 
  select line_number,  61, col061, col062, col063 from gmn_excel_output where col061 is not null and col061 != 'unknown' union all 
  select line_number,  64, col064, col065, col066 from gmn_excel_output where col064 is not null and col064 != 'unknown' union all 
  select line_number,  67, col067, col068, col069 from gmn_excel_output where col067 is not null and col067 != 'unknown' union all 
  select line_number,  70, col070, col071, col072 from gmn_excel_output where col070 is not null and col070 != 'unknown' union all 
  select line_number,  73, col073, col074, col075 from gmn_excel_output where col073 is not null and col073 != 'unknown' union all 
  select line_number,  76, col076, col077, col078 from gmn_excel_output where col076 is not null and col076 != 'unknown' union all 
  select line_number,  79, col079, col080, col081 from gmn_excel_output where col079 is not null and col079 != 'unknown' union all 
  select line_number,  82, col082, col083, col084 from gmn_excel_output where col082 is not null and col082 != 'unknown' union all 
  select line_number,  85, col085, col086, col087 from gmn_excel_output where col085 is not null and col085 != 'unknown' union all 
  select line_number,  88, col088, col089, col090 from gmn_excel_output where col088 is not null and col088 != 'unknown' union all 
  select line_number,  91, col091, col092, col093 from gmn_excel_output where col091 is not null and col091 != 'unknown' union all 
  select line_number,  94, col094, col095, col096 from gmn_excel_output where col094 is not null and col094 != 'unknown' union all 
  select line_number,  97, col097, col098, col099 from gmn_excel_output where col097 is not null and col097 != 'unknown' union all 
  select line_number, 100, col100, col101, col102 from gmn_excel_output where col100 is not null and col100 != 'unknown' union all 
  select line_number, 103, col103, col104, col105 from gmn_excel_output where col103 is not null and col103 != 'unknown' union all 
  select line_number, 106, col106, col107, col108 from gmn_excel_output where col106 is not null and col106 != 'unknown' union all 
  select line_number, 109, col109, col110, col111 from gmn_excel_output where col109 is not null and col109 != 'unknown' union all 
  select line_number, 112, col112, col113, col114 from gmn_excel_output where col112 is not null and col112 != 'unknown' union all 
  select line_number, 115, col115, col116, col117 from gmn_excel_output where col115 is not null and col115 != 'unknown' union all 
  select line_number, 118, col118, col119, col120 from gmn_excel_output where col118 is not null and col118 != 'unknown' union all 
  select line_number, 121, col121, col122, col123 from gmn_excel_output where col121 is not null and col121 != 'unknown' union all 
  select line_number, 124, col124, col125, col126 from gmn_excel_output where col124 is not null and col124 != 'unknown' union all 
  select line_number, 127, col127, col128, col129 from gmn_excel_output where col127 is not null and col127 != 'unknown' union all 
  select line_number, 130, col130, col131, col132 from gmn_excel_output where col130 is not null and col130 != 'unknown' union all 
  select line_number, 133, col133, col134, col135 from gmn_excel_output where col133 is not null and col133 != 'unknown' union all 
  select line_number, 136, col136, col137, col138 from gmn_excel_output where col136 is not null and col136 != 'unknown' union all 
  select line_number, 139, col139, col140, col141 from gmn_excel_output where col139 is not null and col139 != 'unknown' union all 
  select line_number, 142, col142, col143, col144 from gmn_excel_output where col142 is not null and col142 != 'unknown' union all 
  select line_number, 145, col145, col146, col147 from gmn_excel_output where col145 is not null and col145 != 'unknown' union all 
  select line_number, 148, col148, col149, col150 from gmn_excel_output where col148 is not null and col148 != 'unknown' union all 
  select line_number, 151, col151, col152, col153 from gmn_excel_output where col151 is not null and col151 != 'unknown' union all 
  select line_number, 154, col154, col155, col156 from gmn_excel_output where col154 is not null and col154 != 'unknown' union all 
  select line_number, 157, col157, col158, col159 from gmn_excel_output where col157 is not null and col157 != 'unknown' union all 
  select line_number, 160, col160, col161, col162 from gmn_excel_output where col160 is not null and col160 != 'unknown' union all 
  select line_number, 163, col163, col164, col165 from gmn_excel_output where col163 is not null and col163 != 'unknown' union all 
  select line_number, 166, col166, col167, col168 from gmn_excel_output where col166 is not null and col166 != 'unknown' union all 
  select line_number, 169, col169, col170, col171 from gmn_excel_output where col169 is not null and col169 != 'unknown' union all 
  select line_number, 172, col172, col173, col174 from gmn_excel_output where col172 is not null and col172 != 'unknown' union all 
  select line_number, 175, col175, col176, col177 from gmn_excel_output where col175 is not null and col175 != 'unknown' union all 
  select line_number, 178, col178, col179, col180 from gmn_excel_output where col178 is not null and col178 != 'unknown' union all 
  select line_number, 181, col181, col182, col183 from gmn_excel_output where col181 is not null and col181 != 'unknown' union all 
  select line_number, 184, col184, col185, col186 from gmn_excel_output where col184 is not null and col184 != 'unknown' union all 
  select line_number, 187, col187, col188, col189 from gmn_excel_output where col187 is not null and col187 != 'unknown' union all 
  select line_number, 190, col190, col191, col192 from gmn_excel_output where col190 is not null and col190 != 'unknown' union all 
  select line_number, 193, col193, col194, col195 from gmn_excel_output where col193 is not null and col193 != 'unknown' union all 
  select line_number, 196, col196, col197, col198 from gmn_excel_output where col196 is not null and col196 != 'unknown' union all 
  select line_number, 199, col199, col200, col201 from gmn_excel_output where col199 is not null and col199 != 'unknown' union all 
  select line_number, 202, col202, col203, col204 from gmn_excel_output where col202 is not null and col202 != 'unknown' union all 
  select line_number, 205, col205, col206, col207 from gmn_excel_output where col205 is not null and col205 != 'unknown' union all 
  select line_number, 208, col208, col209, col210 from gmn_excel_output where col208 is not null and col208 != 'unknown' union all 
  select line_number, 211, col211, col212, col213 from gmn_excel_output where col211 is not null and col211 != 'unknown' union all 
  select line_number, 214, col214, col215, col216 from gmn_excel_output where col214 is not null and col214 != 'unknown' union all 
  select line_number, 217, col217, col218, col219 from gmn_excel_output where col217 is not null and col217 != 'unknown' union all 
  select line_number, 220, col220, col221, col222 from gmn_excel_output where col220 is not null and col220 != 'unknown' union all 
  select line_number, 223, col223, col224, col225 from gmn_excel_output where col223 is not null and col223 != 'unknown' union all 
  select line_number, 226, col226, col227, col228 from gmn_excel_output where col226 is not null and col226 != 'unknown' union all 
  select line_number, 229, col229, col230, col231 from gmn_excel_output where col229 is not null and col229 != 'unknown' union all 
  select line_number, 232, col232, col233, col234 from gmn_excel_output where col232 is not null and col232 != 'unknown' union all 
  select line_number, 235, col235, col236, col237 from gmn_excel_output where col235 is not null and col235 != 'unknown' union all 
  select line_number, 238, col238, col239, col240 from gmn_excel_output where col238 is not null and col238 != 'unknown' union all 
  select line_number, 241, col241, col242, col243 from gmn_excel_output where col241 is not null and col241 != 'unknown' union all 
  select line_number, 244, col244, col245, col246 from gmn_excel_output where col244 is not null and col244 != 'unknown' union all 
  select line_number, 247, col247, col248, col249 from gmn_excel_output where col247 is not null and col247 != 'unknown' union all 
  select line_number, 250, col250, col251, col252 from gmn_excel_output where col250 is not null and col250 != 'unknown' union all 
  select line_number, 253, col253, col254, col255 from gmn_excel_output where col253 is not null and col253 != 'unknown' union all 
  select line_number, 256, col256, col257, col258 from gmn_excel_output where col256 is not null and col256 != 'unknown' union all 
  select line_number, 259, col259, col260, col261 from gmn_excel_output where col259 is not null and col259 != 'unknown' union all 
  select line_number, 262, col262, col263, col264 from gmn_excel_output where col262 is not null and col262 != 'unknown' union all 
  select line_number, 265, col265, col266, col267 from gmn_excel_output where col265 is not null and col265 != 'unknown' union all 
  select line_number, 268, col268, col269, col270 from gmn_excel_output where col268 is not null and col268 != 'unknown' union all 
  select line_number, 271, col271, col272, col273 from gmn_excel_output where col271 is not null and col271 != 'unknown' union all 
  select line_number, 274, col274, col275, col276 from gmn_excel_output where col274 is not null and col274 != 'unknown' union all 
  select line_number, 277, col277, col278, col279 from gmn_excel_output where col277 is not null and col277 != 'unknown' union all 
  select line_number, 280, col280, col281, col282 from gmn_excel_output where col280 is not null and col280 != 'unknown' union all 
  select line_number, 283, col283, col284, col285 from gmn_excel_output where col283 is not null and col283 != 'unknown' union all 
  select line_number, 286, col286, col287, col288 from gmn_excel_output where col286 is not null and col286 != 'unknown' union all 
  select line_number, 289, col289, col290, col291 from gmn_excel_output where col289 is not null and col289 != 'unknown' union all 
  select line_number, 292, col292, col293, col294 from gmn_excel_output where col292 is not null and col292 != 'unknown' union all 
  select line_number, 295, col295, col296, col297 from gmn_excel_output where col295 is not null and col295 != 'unknown';
  
exception when others then
    util.show_error ('Error in procedure parse_csv_by_field_name for csv file: ' || p_csv_file, sqlerrm, true);
end parse_csv_by_field_name;

/******************************************************************************************************************************************************************/

--
-- load_session_info.
--
procedure load_session_info (p_directory in varchar2 default 'GARMIN')
is
l_column_list    varchar2 (2000);
l_data_list      varchar2 (2000);
begin
  set_nls;
  garmin_pkg.remove_csv_files;
  garmin_pkg.sync_fit_files;
  for ff in (select gff.id, gf.file_name, change_extention (extract_filename (gf.file_name), 'csv') csv_file from table (get_file_name (directory_path (p_directory), 'fit')) gf, gmn_fit_files gff
             where instr (gf.file_name, slash, 1, 4) = 0
               and extract_filename (gf.file_name) = gff.file_name and gff.training is null)
  loop
	begin
      garmin_pkg.convert_fit_file_to_csv (ff.file_name, 'FitToCSV-session.bat');		
	  garmin_pkg.parse_csv_by_field_name (ff.csv_file, p_directory);
	  l_column_list  := 'insert into gmn_session_info (fit_id, ';
      l_data_list    := '(' || to_char (ff.id) || ', ';
      for j in (select distinct tc.column_name, tc.data_type, gc.unit, gc.val 
                from gmn_csv_by_field_name gc, user_tab_columns tc where tc.table_name = 'GMN_SESSION_INFO'  and upper (gc.field) = tc.column_name)
      loop
        l_column_list := l_column_list || j.column_name  || ', ';
		l_data_list := l_data_list ||
		case
		  when j.unit      = 'semicircles' then ' garmin_pkg.semicircles_to_lon_lat (' || j.val || '), '
		  when j.data_type = 'NUMBER'      then j.val || ', '
		  when j.data_type = 'DATE'        then ' garmin_pkg.date_offset_to_date (' || j.val || '), '
		  else '''' || j.val || ''', ' end;
      end loop;
      l_column_list := substr (l_column_list, 1, length (l_column_list) - 2) || ') values ';
      l_data_list   := substr (l_data_list  , 1, length (l_data_list)   - 2) || ')'; 
      execute immediate l_column_list || l_data_list;
	  update gmn_fit_files set training = 1 where id = ff.id;
	  commit;
	  exception when others
	  then 
	    dbms_output.put_line (l_column_list);
	    dbms_output.put_line (l_data_list);
	end;
  end loop;

exception when others then

    util.show_error ('Error in procedure load_session_info', sqlerrm, true);
end load_session_info;

/******************************************************************************************************************************************************************/

--
-- load_session_details
--
procedure load_session_details (p_directory in varchar2 default 'GARMIN')
is
l_statement   varchar2 (200);
l_prev_field  varchar2 (200) := '-1';
l_id          integer (6)    := 0;
l_user_id     gmn_devices.user_id%type;
begin
  set_nls;
  garmin_pkg.remove_csv_files;
    for ff in (select fn.file_name, change_extention (extract_filename (fn.file_name), 'csv') csv_file, gf.id fit_id, user_id from table (get_file_name (directory_path (p_directory), 'fit')) fn, gmn_fit_files gf
	           where instr (fn.file_name, slash, 1, 4) = 0
                 and extract_filename (fn.file_name) = gf.file_name and gf.track is null)
    loop
        garmin_pkg.convert_fit_file_to_csv (ff.file_name, 'FitToCSV.bat');
		garmin_pkg.parse_csv_by_field_name (ff.csv_file, p_directory);
		select user_id into l_user_id from gmn_devices where serial# = (select max(val) from gmn_csv_by_field_name where field =  'serial_number');
        l_id := 1;
		
        for j in (select gc.line_number, tc.column_name, gc.field, gc.val, gc.unit, tc.data_type
                  from gmn_csv_by_field_name gc, user_tab_columns tc
                  where tc.table_name = 'GMN_FIT_DATA' and upper (gc.field) = tc.column_name
                  order by gc.line_number, gc.column_position)
        loop
          begin 
          if j.field = 'timestamp' and j.field != l_prev_field
          then
            l_id :=l_id + 1;
	        insert into gmn_fit_data (fit_id, id, person_id, "TIMESTAMP")  values (ff.fit_id, l_id, l_user_id, garmin_pkg.date_offset_to_date (j.val)); 
          else
		    l_statement := 'update gmn_fit_data set ' || j.column_name || '=' ||
		    case 
		      when j.unit      = 'semicircles' then ' garmin_pkg.semicircles_to_lon_lat (' || j.val || ')'
		      when j.data_type = 'NUMBER'      then to_char (j.val)
		      when j.data_type = 'DATE'        then ' garmin_pkg.date_offset_to_date (' || j.val || ')'
			  else '''' || j.val || ''''
		    end || ' where fit_id = ' || ff.fit_id || ' and id = ' || l_id || ' and person_id =' || l_user_id;
		  execute immediate l_statement;
        end if;  
		
	  exception when others
	  then
	    dbms_output.put_line (l_statement);
		util.show_error ('Error in procedure load_session_details in inner loop', sqlerrm);
	  end;
	  l_prev_field := j.field;
	  end loop;
	  update gmn_fit_files set track = 1, user_id = l_user_id where id = ff.fit_id;
	  commit;
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
  into l_heartrate from gmn_heartrate_zones where person_id = p_person_id and loaded = (select max (loaded) from gmn_heartrate_zones where person_id = p_person_id);
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
	  when p_field = 7  then l_cells (l_row) (j.lap).cell := to_char (j.enhanced_max_speed, '990D99'); 
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
-- Upload json files downloaded from Garmin website in the database
--
procedure load_json_files (p_person_id in integer, p_directory in varchar2 default 'GARMIN')
is
l_bfile       bfile;
l_blob        blob := empty_blob;
l_dest_offset integer;
l_src_offset  integer;
begin
  for j in (select substr (file_name, 16) my_file from table (get_file_name (directory_path (p_directory), 'json')))
  loop
--    if j.my_file like '%heartRateZones%' or j.my_file like '%sleepData%' or j.my_file like '%gear%'  or j.my_file like '%personalRecord%' or j.my_file like  '%userBioMetricProfileData%' or j.my_file like '%user_profile%' or 
--       j.my_file like  '%UDSFile%' or j.my_file like  '%user_settings%' or j.my_file like  '%courses%' or j.my_file like  '%Predictions%' 
--   then
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
--	end if;
  end loop;
  commit;

exception when others then 
   util.show_error ('Error in procedure load_json_files for person_id: ' || p_person_id, sqlerrm);
end load_json_files;

/******************************************************************************************************************************************************************/

--
-- Copy json data to tables
--
procedure evaluate_json_data
is
begin
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

for j in (select person_id, to_date (calendardate, 'YYYY-MM-DD') calendardate, deviceid, acuteload, acwrfactorfeedback, acwrfactorpercent, feedbacklong, feedbackshort, hrvfactorfeedback                                                    
            , hrvfactorpercent, hrvweeklyaverage, ready_level, recoverytime, recoverytimefactorfeedback, recoverytimefactorpercent, score
            , sleephistoryfactorfeedback, sleephistoryfactorpercent, sleepscore, sleepscorefactorfeedback, sleepscorefactorpercent
            , stresshistoryfactorfeedback, stresshistoryfactorpercent, validsleep
            from v_gmn_json_training_readyness
            where (person_id, to_date (calendardate, 'YYYY-MM-DD')) not in (select person_id, calendardate from gmn_training_readyness))
loop 
  begin
    insert into gmn_training_readyness
     ( person_id, calendardate, device_id, acuteload, acwrfactorfeedback, acwrfactorpercent, feedbacklong, feedbackshort, hrvfactorfeedback                                               
     , hrvfactorpercent, hrvweeklyaverage, ready_level, recoverytime, recoverytimefactorfeedback, recoverytimefactorpercent, score
     , sleephistoryfactorfeedback, sleephistoryfactorpercent, sleepscore, sleepscorefactorfeedback, sleepscorefactorpercent
     , stresshistoryfactorfeedback, stresshistoryfactorpercent, validsleep)
    values 
     ( j.person_id, j.calendardate, j.deviceid, j.acuteload, j.acwrfactorfeedback, j.acwrfactorpercent, j.feedbacklong, j.feedbackshort, j.hrvfactorfeedback                                               
     , j.hrvfactorpercent, j.hrvweeklyaverage, j.ready_level, j.recoverytime, j.recoverytimefactorfeedback, j.recoverytimefactorpercent, j.score
     , j.sleephistoryfactorfeedback, j.sleephistoryfactorpercent, j.sleepscore, j.sleepscorefactorfeedback, j.sleepscorefactorpercent
     , j.stresshistoryfactorfeedback, j.stresshistoryfactorpercent, j.validsleep);
  
  exception when dup_val_on_index 
  then null;
  end;
end loop;

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

exception when others then 
   util.show_error ('Error in procedure evaluate_json_data', sqlerrm);
end evaluate_json_data;

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
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
       json_table (jd.json_clob, '$[*]'
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
where hydration_calendarDate is not null and jd.name like  '%UDSFile%';


create table gmn_hydration
( uuid                                                                                                       varchar2 (40)
, person_id                                                                               number (4)
, version                                                                                                  number (8)
, vigorousintensityminutes                                     number (4)
, wellnessactivekilocalories                      number (8)
, wellnessdistancemeters                                       number (8)
, wellnessendtimegmt                                             date -- varchar2 (40)  -- Date 2023-08-07T22:00:00.0
, wellnessendtimelocal                                            date -- varchar2 (40)  -- Date 2023-08-08T00:00:00.0
, wellnesskilocalories                                               number (8)
, wellnessstarttimegmt                                            date -- varchar2 (40)  -- Date 2023-08-06T22:00:00.0
, wellnessstarttimelocal                            date -- varchar2 (40)  -- Date 2023-08-07T00:00:00.0
, wellnesstotalkilocalories                                       number (8)
, calendardate                                                                         date -- varchar2 (40) -- Date 2023-08-03
, goalinml                                                                                                number (8)
, lastentrytimestamplocal                                       date -- varchar2 (40) -- Date 2023-08-03T11:34:11.0
, sweatlossinml                                                                       number (8)
, valueinml                                                                                             number (8)
, maxheartrate                                                                        number (4)
, minavgheartrate                                                                   number (2)
, minheartrate                                                                         number (2)
, moderateintensityminutes                                   number (4)
, remainingkilocalories                                            number (8)
, avgwakingrespirationvalue                                  number (4)
, highestrespirationvalue                                        number (4)
, latestrespirationtimegmt                                      date  -- varchar2 (40) -- Date 2023-08-07T22:00:00.0
, latestrespirationvalue                             number (4)
, lowestrespirationvalue                                         number (4)
, restingcaloriesfromactivity                     number (8)
, restingheartrate                                                                    number (2)
, restingheartratetimestamp                                  number (16) -- Date 1691445600000
, totaldistancemeters                                               number (8)
, totalkilocalories                                                                    number (8)
, totalsteps                                                                               number (8)
, userintensityminutesgoal                                     number (4));

alter table gmn_hydration add constraint gmn_hydration_pk primary key (uuid) using index;

userprofilepk      113255650      --> 1
userprofilepk      114954293      --> 2

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



--------------------------- Biometrics profile

create or replace view v_gmn_json_biometrics_profile
as 
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
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
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
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
where accessControl is not null and jd.name like  '%courses%';

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
and (coursename,person_id) not in (select coursename,person_id from v_gmn_json_courses  group by coursename,person_id);

--------------------------- Gear

-- Gear
create or replace view v_gmn_json_gear
as 
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
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
from v_gmn_json_gear
where (person_id, gearpk) not in (select person_id, gear_id from gmn_gear);

--------------------------- Heartrates zones

create or replace view v_gmn_json_heartrate_zones
as 
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
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
, zone5           number (4)
, loaded          date);

alter table gmn_heartrate_zones add constraint gmn_heartrate_zones_pk primary key (person_id, sport, trainingmethod, loaded) using index;

insert into gmn_heartrate_zones (person_id, sport, trainingmethod, zone1, zone2, zone3, zone4, zone5, loaded)        
select distinct person_id, sport, trainingmethod, zone1floor, zone2floor, zone3floor, zone4floor, zone5floor, loaded from v_gmn_json_heartrate_zones
where (person_id, sport, trainingmethod, loaded) not in (select person_id, sport, trainingmethod, loaded from gmn_heartrate_zones);

--------------------------- Personal records

create or replace view v_gmn_json_personal_records
as 
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
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
where activityId is not null and jd.name like '%personalRecord%';

create table gmn_personal_records
( person_id       number (4)
, record_type     varchar2 (40)
, confirmed       varchar2 (10)
, current_v       varchar2 (10)
, created         date
, value           number (10));

alter table gmn_personal_records add constraint gmn_personal_records_pk primary key (person_id, record_type, current_v, value) using index;

insert into gmn_personal_records (person_id, record_type, confirmed, current_v, created, value)
select distinct person_id, personalrecordtype, confirmed, current_v, to_date(createddate, 'YYYY-MM-DD'), value from  v_gmn_json_personal_records
where personalrecordtype is not null
and (person_id, personalRecordType, current_v, value) not in (select person_id, record_type, current_v, value from gmn_personal_records);



--------------------------- Runrace predictions

 create or replace view v_gmn_json_runrace_predictions
 as
  select distinct jt."CALENDARDATE",jt."DEVICEID",jt."RACETIME10K",jt."RACETIME5K",jt."RACETIMEHALF",jt."RACETIMEMARATHON",jt."TIMESTAMP",jt."USERPROFILEPK", jd.person_id, jd.loaded from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
calendarDate varchar2 (40) path '$.calendarDate',
deviceId number (16) path '$.deviceId',
raceTime10K number (4) path '$.raceTime10K',
raceTime5K number (4) path '$.raceTime5K',
raceTimeHalf number (8) path '$.raceTimeHalf',
raceTimeMarathon number (8) path '$.raceTimeMarathon',
timestamp varchar2 (40) path '$.timestamp',
userProfilePK number (16) path '$.userProfilePK') jt
where calendarDate is not null and jd.name like  '%RunRacePredictions%';

create table gmn_runrace_predictions
( person_id     number (4)
, cal_date      date
, racetime_5k   number (6)
, racetime_10k  number (6)
, half_marathon number (6)
, marathon      number (6));

alter table gmn_runrace_predictions add constraint gmn_runrace_predictions_pk primary key (person_id, cal_date) using index;

insert into gmn_runrace_predictions (person_id, cal_date, racetime_5k, racetime_10k, half_marathon, marathon)
select person_id, to_date (calendardate, 'YYYY-MM-DD'), min(racetime5k), min(racetime10k), min(racetimehalf), min(racetimemarathon)
from v_gmn_json_runrace_predictions
where (person_id, to_date (calendardate, 'YYYY-MM-DD')) not in (select person_id, cal_date from gmn_runrace_predictions)
group by person_id, to_date (calendardate, 'YYYY-MM-DD');



--------------------------- User profile

create or replace view v_gmn_json_user_profile
as 
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
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

alter  table gmn_user_profile add constraint gmn_user_profile_pk primary key (person_id) using index;

insert into gmn_user_profile( person_id, birthdate, email, first_name, last_name, gender, username)
select person_id, to_date(birthdate, 'YYYY-MM-DD'), emailaddress, firstname,lastname, gender,username from v_gmn_json_user_profile
where person_id not in (select person_id from gmn_user_profile);


--------------------------- User settings

create or replace view v_gmn_json_user_settings
as 
select distinct jt.*, jd.person_id, jd.loaded from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
handedness varchar2 (40) path '$.handedness',
preferredLocale varchar2 (40) path '$.preferredLocale') jt
where handedness is not null and jd.name like  '%user_settings%';

create table gmn_user_settings
( person_id     number (4)
, handedness    varchar2 (10)
, locale        varchar2 (10)
, loaded        date);

insert into gmn_user_settings (person_id, handedness, locale, loaded)
select person_id, handedness, preferredlocale, loaded from v_gmn_json_user_settings
where (person_id, loaded) not in (select person_id, loaded from gmn_user_settings);

alter table gmn_user_settings add constraint gmn_user_settings_pk primary key (person_id, loaded) using index;

--------------------------- Sleep data

-- Sleep data
create or replace view v_gmn_json_sleep_data
as 
select distinct jt.*, to_date(jt.CalendarDate, 'YYYY-MM-DD') cal_date, 
to_date(substr(sleependtimestampgmt, 1, 10) || substr(sleependtimestampgmt, 12, 8), 'YYYY-MM-DDHH24:MI:SS') sleep_end,
to_date(substr(sleepstarttimestampgmt, 1, 10) || substr(sleepstarttimestampgmt, 12, 8), 'YYYY-MM-DDHH24:MI:SS') sleep_start, jd.person_id, jd.loaded from gmn_json_data  jd,
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
where  name like '%sleepData%' and avgSleepStress is not null or averageRespiration is not null or awakeCount is not null;


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


alter table gmn_sleep_data add constraint gmn_sleep_data_pk primary key (person_id, cal_date) using index;
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
cal_date, sleep_end, sleep_start, person_id from v_gmn_json_sleep_data
where (person_id, cal_date) not in (select person_id, cal_date from gmn_sleep_data);           

---------------------------

-- Do not add the loaded column to this view.
create or replace force view v_gmn_json_training_readyness 
as
  select distinct jt.ACUTELOAD,jt.ACWRFACTORFEEDBACK,jt.ACWRFACTORPERCENT,jt.CALENDARDATE,jt.DEVICEID,jt.FEEDBACKLONG,jt.FEEDBACKSHORT,jt.HRVFACTORFEEDBACK,jt.HRVFACTORPERCENT,jt.HRVWEEKLYAVERAGE,jt.READY_LEVEL,jt.RECOVERYTIME,jt.RECOVERYTIMEFACTORFEEDBACK,jt.RECOVERYTIMEFACTORPERCENT,jt.SCORE,jt.SLEEPHISTORYFACTORFEEDBACK,jt.SLEEPHISTORYFACTORPERCENT,jt.SLEEPSCORE,jt.SLEEPSCOREFACTORFEEDBACK,jt.SLEEPSCOREFACTORPERCENT,jt.STRESSHISTORYFACTORFEEDBACK,jt.STRESSHISTORYFACTORPERCENT,jt.TIMESTAMP,jt.TIMESTAMPLOCAL,jt.USERPROFILEPK,jt.VALIDSLEEP, jd.person_id from gmn_json_data  jd,
       json_table(jd.json_clob, '$[*]'
         columns
acuteLoad number (4) path '$.acuteLoad',
acwrFactorFeedback varchar2 (40) path '$.acwrFactorFeedback',
acwrFactorPercent number (4) path '$.acwrFactorPercent',
calendarDate varchar2 (40) path '$.calendarDate',
deviceId number (16) path '$.deviceId',
feedbackLong varchar2 (40) path '$.feedbackLong',
feedbackShort varchar2 (40) path '$.feedbackShort',
hrvFactorFeedback varchar2 (40) path '$.hrvFactorFeedback',
hrvFactorPercent number (4) path '$.hrvFactorPercent',
hrvWeeklyAverage number (4) path '$.hrvWeeklyAverage',
ready_level varchar2 (40) path '$.level',
recoveryTime number (4) path '$.recoveryTime',
recoveryTimeFactorFeedback varchar2 (40) path '$.recoveryTimeFactorFeedback',
recoveryTimeFactorPercent number (2) path '$.recoveryTimeFactorPercent',
score number (2) path '$.score',
sleepHistoryFactorFeedback varchar2 (40) path '$.sleepHistoryFactorFeedback',
sleepHistoryFactorPercent number (2) path '$.sleepHistoryFactorPercent',
sleepScore number (2) path '$.sleepScore',
sleepScoreFactorFeedback varchar2 (40) path '$.sleepScoreFactorFeedback',
sleepScoreFactorPercent number (2) path '$.sleepScoreFactorPercent',
stressHistoryFactorFeedback varchar2 (40) path '$.stressHistoryFactorFeedback',
stressHistoryFactorPercent number (2) path '$.stressHistoryFactorPercent',
timestamp varchar2 (40) path '$.timestamp',
timestampLocal varchar2 (40) path '$.timestampLocal',
userProfilePK number (16) path '$.userProfilePK',
validSleep varchar2 (10) path '$.validSleep') jt
where acuteLoad is not null and jd.name like  '%TrainingReadinessDTO%';


create table gmn_training_readyness
( person_id                                                                              number (4)
, calendardate                                                           date
, device_id                                                                                number (12)
, acuteload                                                                               number (4)
, acwrfactorfeedback                                 varchar2 (40)
, acwrfactorpercent                                                 number (4)
, feedbacklong                                                           varchar2 (40)
, feedbackshort                                                         varchar2 (40)
, hrvfactorfeedback                                                  varchar2 (40)
, hrvfactorpercent                                                    number (4)
, hrvweeklyaverage                                                  number (4)
, ready_level                                                              varchar2 (40)
, recoverytime                                                           number (4)
, recoverytimefactorfeedback   varchar2 (40)
, recoverytimefactorpercent                    number (2)
, score                                                                                       number (2)
, sleephistoryfactorfeedback     varchar2 (40)
, sleephistoryfactorpercent                      number (2)
, sleepscore                                                                number (2)
, sleepscorefactorfeedback                      varchar2 (40)
, sleepscorefactorpercent                         number (2)
, stresshistoryfactorfeedback    varchar2 (40)
, stresshistoryfactorpercent       number (2)
, validsleep                                                                 varchar2 (10));

alter table gmn_training_readyness add constraint gmn_training_readyness_pk primary key (person_id, calendardate) using index;

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


-----------------------------------------

create or replace view v_gmn_json_TrainingHistory
as 
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
deviceId                      number (16)          path '$.deviceId',
timestamp                     varchar2 (32)        path '$.timestamp',
calendarDate                  varchar2 (16)        path '$.calendarDate',
userProfilePK                 number (16)          path '$.userProfilePK',
trainingStatus                varchar2 (16)        path '$.trainingStatus',
fitnessLevelTrend             varchar2 (16)        path '$.fitnessLevelTrend',
trainingStatus2FeedbackPhrase varchar2 (16)        path '$.trainingStatus2FeedbackPhrase')) as jc
where name like 'TrainingHistory%.json';


---------------------------------- metrics_metadata

create or replace view v_gmn_json_metrics_metadata
as
select jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
sport                         varchar2 (8)         path '$.sport',
maxMet                        number (32)          path '$.maxMet',
deviceId                      number (16)          path '$.deviceId',
subSport                      varchar2 (8)         path '$.subSport',
vo2MaxValue                   number (4)           path '$.vo2MaxValue',
calendarDate                  varchar2 (16)        path '$.calendarDate',
userProfilePK                 number (16)          path '$.userProfilePK',
calibratedData                number (1)           path '$.calibratedData',
maxMetCategory                varchar2 (8)         path '$.maxMetCategory',
updateTimestamp               varchar2 (32)        path '$.updateTimestamp')) jc
 where t.name like 'MetricsMaxMetData_%.json';

-------------------------------------------------------------

create or replace view v_gmn_json_AbnormalHrEvents
as 
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
deviceId                      number (16)          path '$.deviceId',
calendarDate                  varchar2 (16)        path '$.calendarDate',
abnormalHrValue               number (4)           path '$.abnormalHrValue',
abnormalHrEventGMT            varchar2 (32)        path '$.abnormalHrEventGMT',
abnormalHrThresholdValue      number (4)           path '$.abnormalHrThresholdValue')) jc
 where name like '%AbnormalHrEvents.json';
 
-------------------------------------------------------------

create or replace view v_gmn_json_HydrationLog
as 
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
uuid                     varchar2 (64)        path '$.uuid.uuid',
capped                        number (8)           path '$.capped',
duration                      number (32)          path '$.duration',
valueInML                     number (8)           path '$.valueInML',
activityId                    number (16)          path '$.activityId',
calendarDate                  varchar2 (16)        path '$.calendarDate',
userProfilePK                 number (16)          path '$.userProfilePK',
timestampLocal                varchar2 (32)        path '$.timestampLocal',
hydrationSource               varchar2 (16)        path '$.hydrationSource',
persistedTimestampGMT         varchar2 (32)        path '$.persistedTimestampGMT',
estimatedSweatLossInML        number (8)           path '$.estimatedSweatLossInML')) as jc
where name like 'HydrationLogFile%.json';

-------------------------------------------------------------

create or replace view v_gmn_json_AcuteTrainingLoad
as 
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
deviceId                      number (16)          path '$.deviceId',
timestamp                     number (16)          path '$.timestamp',
acwrStatus                    varchar2 (16)        path '$.acwrStatus',
acwrPercent                   number (4)           path '$.acwrPercent',
calendarDate                  number (16)          path '$.calendarDate',
userProfilePK                 number (16)          path '$.userProfilePK',
acwrStatusFeedback            varchar2 (16)        path '$.acwrStatusFeedback',
dailyTrainingLoadAcute        number (4)           path '$.dailyTrainingLoadAcute',
dailyTrainingLoadChronic      number (4)           path '$.dailyTrainingLoadChronic',
dailyAcuteChronicWorkloadRatio number (4)           path '$.dailyAcuteChronicWorkloadRatio')) as jc
where name like 'MetricsAcuteTrainingLoad%.json';

-------------------------------------------------------------

create or replace view v_gmn_json_HeatAltitudeAcclimation
as 
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
deviceId                      number (16)          path '$.deviceId',
heatTrend                     varchar2 (16)        path '$.heatTrend',
timestamp                     varchar2 (32)        path '$.timestamp',
calendarDate                  varchar2 (16)        path '$.calendarDate',
prevAltitude                  number (2)           path '$.prevAltitude',
altitudeTrend                 varchar2 (16)        path '$.altitudeTrend',
userProfilePK                 number (16)          path '$.userProfilePK',
currentAltitude               number (4)           path '$.currentAltitude',
altitudeAcclimation           number (4)           path '$.altitudeAcclimation',
acclimationPercentage         number (1)           path '$.acclimationPercentage',
heatAcclimationTimestamp      varchar2 (32)        path '$.heatAcclimationTimestamp',
heatAcclimationPercentage     number (2)           path '$.heatAcclimationPercentage',
prevAcclimationPercentage     number (1)           path '$.prevAcclimationPercentage',
previousAltitudeAcclimation   number (4)           path '$.previousAltitudeAcclimation',
altitudeAcclimationTimestamp  varchar2 (32)        path '$.altitudeAcclimationTimestamp',
previousHeatAcclimationTimestamp varchar2 (32)        path '$.previousHeatAcclimationTimestamp',
altitudeAcclimationLocalTimestamp varchar2 (32)        path '$.altitudeAcclimationLocalTimestamp',
previousHeatAcclimationPercent number (2)           path '$.previousHeatAcclimationPercentage',
previousAltitudeAcclimationTime varchar2 (32)        path '$.previousAltitudeAcclimationTimestamp')) as jc
where name like 'MetricsHeatAltitudeAcclimation%.json';


-------------------------------------------------------------

create or replace view v_gmn_json_CalendarItems
as 
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
calendardate           varchar2 (16)        path '$.calendarEvents.date',
ev_name           varchar2 (16)        path '$.calendarEvents.name',
race           number (8)           path '$.calendarEvents.race',
eventId        number (8)           path '$.calendarEvents.eventId',
location       varchar2 (32)        path '$.calendarEvents.location',
timeZone       varchar2 (16)        path '$.calendarEvents.timeZone',
eventType      varchar2 (8)         path '$.calendarEvents.eventType',
startPointLat  number (16)          path '$.calendarEvents.startPointLat',
startPointLon  number (8)           path '$.calendarEvents.startPointLon',
startTimeHhMm  varchar2 (8)         path '$.calendarEvents.startTimeHhMm',
completionTarget number (8)           path '$.calendarEvents.completionTargetValueNormalized',
event_id number (8)           path '$.calendarEventsParticipation.eventId',
primary number (8)           path '$.calendarEventsParticipation.primary',
trainingEvent  number (4)           path '$.calendarEventsParticipation.trainingEvent')) as jc
where name like 'CalendarItems.json';

-------------------------------------------------------------

create or replace view v_gmn_json_events
as
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
locale                                   varchar2 (8)         path '$.locale',
testMode                                 number (8)           path '$.testMode',
request                        varchar2 (4000)      path '$.eventData.request',
resourceId                     varchar2 (16)        path '$.eventData.resourceId',
newLocation                    varchar2 (2)         path '$.eventData.newLocation',
oldLocation                    varchar2 (2)         path '$.eventData.oldLocation',
EVENT_ACTION                   varchar2 (16)        path '$.eventData.EVENT_ACTION',
EVENT_SOURCE                   varchar2 (128)       path '$.eventData.EVENT_SOURCE',
resourceType                   varchar2 (8)         path '$.eventData.resourceType',
preferenceKey                  varchar2 (64)        path '$.eventData.preferenceKey',
preferenceValue                varchar2 (2048)      path '$.eventData.preferenceValue',
COMPLETION_STATUS              varchar2 (16)        path '$.eventData.COMPLETION_STATUS',
profileVisibility              varchar2 (16)        path '$.eventData.profileVisibility',
connectionRequestId            varchar2 (16)        path '$.eventData.connectionRequestId',
connectionRequesteeId          varchar2 (16)        path '$.eventData.connectionRequesteeId',
connectionRequestorId          varchar2 (16)        path '$.eventData.connectionRequestorId',
eventType                                varchar2 (64)        path '$.eventType',
userAgent                                varchar2 (128)       path '$.userAgent',
platformId                               varchar2 (32)        path '$.platformId',
referenceId                              varchar2 (64)        path '$.referenceId',
sourceSystem                             varchar2 (64)        path '$.sourceSystem',
eventDateTime                            varchar2 (32)        path '$.eventDateTime',
referenceType                            varchar2 (16)        path '$.referenceType',
locationCountry                          varchar2 (4)         path '$.locationCountry',
sourceSystemVersion                      varchar2 (16)        path '$.sourceSystemVersion')) as jc
where name like 'events.json';

-------------------------------------------------------------

create or replace view v_gmn_json_customer
as
select t.person_id, jc.* from gmn_json_data t, 
json_table (t.json_clob,  '$[*]'
COLUMNS (
id                                       varchar2 (4)         path '$.id',
type                                     varchar2 (4)         path '$.type',
active                                   number (4)           path '$.active',
gender                                   varchar2 (4)         path '$.gender',
locale                                   varchar2 (8)         path '$.locale',
version                                  number (2)           path '$.version',
fullName                                 varchar2 (16)        path '$.fullName',
lastName                                 varchar2 (16)        path '$.lastName',
username                                 varchar2 (32)        path '$.username',
firstName                                varchar2 (4)         path '$.firstName',
loginInfo                                varchar2 (4)         path '$.loginInfo',
middleName                               varchar2 (4)         path '$.middleName',
namePrefix                               varchar2 (4)         path '$.namePrefix',
nameSuffix                               varchar2 (4)         path '$.nameSuffix',
toBePurged                               number (8)           path '$.toBePurged',
companyName                              varchar2 (4)         path '$.companyName',
createdDate                              varchar2 (32)        path '$.createdDate',
displayName                              varchar2 (4)         path '$.displayName',
legacyAccount                            number (8)           path '$.legacyAccount',
primaryAddress                           varchar2 (4)         path '$.primaryAddress',
accountVerified                          number (8)           path '$.accountVerified',
customerLocation_verified                number (4)           path '$.customerLocation.verified',
customerLocation_countryCode             varchar2 (2)         path '$.customerLocation.countryCode',
customerLocation_verifiedTimestamp       varchar2 (32)        path '$.customerLocation.verifiedTimestamp',
lastUpdateDateTime                       varchar2 (32)        path '$.lastUpdateDateTime',
nextPurgeCheckDate                       varchar2 (4)         path '$.nextPurgeCheckDate',
primaryPhoneNumber                       varchar2 (4)         path '$.primaryPhoneNumber',
primaryEmailAddress_type                 varchar2 (4)         path '$.primaryEmailAddress.type',
primaryEmailAddress_primary              number (4)           path '$.primaryEmailAddress.primary',
primaryEmailAddress_emailAddress         varchar2 (32)        path '$.primaryEmailAddress.emailAddress',
primaryEmailAddress_emailVerified        number (8)           path '$.primaryEmailAddress.emailVerified',
primaryEmailAddress_emailAddressId       varchar2 (4)         path '$.primaryEmailAddress.emailAddressId',
primaryEmailAddress_emailVerificationTim varchar2 (4)         path '$.primaryEmailAddress.emailVerificationTimestamp',
accountVerificationDate                  varchar2 (4)         path '$.accountVerificationDate')) as jc
where name like 'customer.json';

-------------------------------------------------------------

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


-- Daylight saving time correction
create table gmn_dst_data (year number (4), dst_start number (2), dst_end number (2));
-- create unique index gmn_dst_data_uk1 on gmn_dst_data (year); Add timezone region. Properties table?
   
   
create or replace view v_gmn_dst_data
as
with idst as (
select year,
   to_date (to_char (year) || '03' || to_char (dst_start) || '03', 'YYYYMMDDHH24') dst_start,
   to_date (to_char (year) || '10' || to_char (dst_end  ) || '03', 'YYYYMMDDHH24') dst_end
   from gmn_dst_data)   
select
  si.start_time training_date,
  fit_id,
  case when si.start_time between idst.dst_start and idst.dst_end then 'Summertime' else 'Wintertime' end Season,
  case when si.start_time between idst.dst_start and idst.dst_end then to_char(si.start_time + 2/24, 'HH24:MI') else to_char(si.start_time + 1/24, 'HH24:MI') end start_time,
  si.sport_profile_name sport_profile,
  u.first_name,
  u.last_name
from gmn_session_info si
join idst               on to_number (to_char (si.start_time, 'YYYY')) = idst.year
join gmn_fit_files ff   on si.fit_id  = ff.id 
join gmn_user_profile u on ff.user_id = u.person_id
order by training_date;
  
/*
select * from v$timezone_names where tzname like 'Europe%';
select tz_offset('Europe/Zurich') from dual;
 
select start_time, cast(from_tz(cast(start_time as timestamp), 'CET') at time zone 'Europe/Amsterdam'
  as date) as your_column_alias from gmn_session_info order by fit_id;
*/

https://www.youtube.com/watch?v=JzDsvuEBRaU

Source code
https://drive.google.com/drive/folders/1fpcG9lA2Wy9axb4arsDI1TDDEyQoGOPZ


begin
update gmn_json_data set view_name =  'v_gmn_json_hydration'  where name like  '%UDSFile%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_biometrics_profile'  where name like  '%userBioMetricProfileData%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_courses'  where name like  '%courses%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_gear'  where name like  '%gear%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_heartrate_zones'  where name like  '%heartRateZones%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_personal_records'  where name like  '%personalRecord%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_user_profile'  where name like  '%user_profile%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_user_settings'  where name like  '%user_settings%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_sleep_data'  where name like  '%sleepData%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_training_readyness'  where name like '%TrainingReadinessDTO%' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_TrainingHistory'  where name like 'TrainingHistory%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_metrics_metadata'  where name like 'MetricsMaxMetData_%.json' and view_name is null;
update gmn_json_data set view_name =  'v_gmn_json_runrace_predictions'  where name like '%RunRacePredictions%' and view_name is null;
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