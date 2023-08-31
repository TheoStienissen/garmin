
Garmin data export: https://www.garmin.com/en-GB/account/datamanagement/exportdata/

create table garmin (
id                         integer generated always as identity,
persoon                    varchar2 (20),
Activiteittype             varchar2 (50),
Datum                      date,
Favoriet                   varchar2 (5),
Titel                      varchar2 (50),
Afstand                    number (5, 2),
Calorieen                  number (6),
Tijd                       interval day to second,
Gem_HS                     number (3),
Max_HS                     number (3),
Aeroob_TE                  number (2,1),
Gemiddelde_fietscadans     varchar2 (10),
Max_fietscadans            varchar2 (10),
Gemiddelde_snelheid        number (4, 1),
Max_snelheid               number (4, 1),
Totale_stijging            number (4),
Totale_daling              number (4),
Gem_staplengte             number (4,2),
Gemiddelde_verticale_ratio number (4,2),
Gem_verticale_oscillatie   number (4,2),
Gem_grondcontacttijd       number (6, 2),
Gem_GAP                    varchar2 (10),
Training_Stress_Score      number (6,2),
Gem_vermogen               number (6,2),
Max_vermogen               number (6,2),
Grit                       number (6,2),
Flow                       number (6,2),
Gemiddelde_Swolf           number (4,2),
Gem_slagsnelheid           number (4,2),
Totaal_herhalingen         number (3),
Totaal_sets                number (3),
Duiktijd                   varchar2 (10),
Min_temp                   number (3,1),
Oppervlakte_interval       varchar2 (10),
Decompressie               varchar2 (10),
Beste_rondetijd            interval day to second,
Aantal_ronden              number (3),
Max_temp                   number (3,1),
Tijd_bewogen               interval day to second,
Verstreken_tijd            interval day to second,
Minimum_hoogte             number (4),
Maximum_hoogte             number (4));


alter table garmin add constraint garmin_pk primary key (id);
create unique index garmin_uk1 on garmin (persoon, Activiteittype, Datum);


create table garmin_details (
garmin_id                  number (10),
Ronden                     number (4),
Tijd                       interval day to second,
Totale_tijd	               interval day to second,
Afstand                    number (5, 2),
Gemiddelde_snelheid        number (4, 1),
Gem_GAP                    varchar2 (10),
Gem_HS                     number (3),
Max_HS                     number (3),
Totale_stijging            number (4),
Totale_daling              number (4),
Calorieen                  number (6),
Max_snelheid               number (4, 1),
Tijd_bewogen	           interval day to second,
Gem_bewogen_snelheid       number (4, 1));


alter table garmin_details add constraint garmin_datails_pk primary key (garmin_id, Ronden);
alter table garmin_details add constraint garmin_details_fk1 foreign key (garmin_id) references garmin (id) on delete cascade;

create directory garmin as 'C:\Work\garmin';
set serveroutput on size unlimited

create or replace procedure load_garmin_data (p_user in varchar2, p_garmin_id in integer default null)
is 
  type string_ty   is table of varchar2 (100) index by binary_integer;
  l_string_row                 string_ty;
  l_fhandle                    utl_file.file_type;
  l_string                     varchar2 (4000);
  l_next                       varchar (50);
  l_pos                        number (4);
  l_return                     varchar2 (50);
  l_persoon                    varchar2 (20);
  l_Activiteittype             varchar2 (50);
  l_Datum                      date;
  l_Favoriet                   varchar2 (5);
  l_Titel                      varchar2 (50);
  l_Afstand                    number (5, 2);
  l_Calorieen                  number (6);
  l_Tijd                       interval day to second;
  l_Gem_HS                     number (3);
  l_Max_HS                     number (3);
  l_Aeroob_TE                  number (2,1);
  l_Gemiddelde_fietscadans     varchar2 (10);
  l_Max_fietscadans            varchar2 (10);
  l_Gemiddelde_snelheid        number (4, 1);
  l_Max_snelheid               number (4, 1);
  l_Totale_stijging            number (4);
  l_Totale_daling              number (4);
  l_Gem_staplengte             number (4,2);
  l_Gemiddelde_verticale_ratio number (4,2);
  l_Gem_verticale_oscillatie   number (4,2);
  l_Gem_grondcontacttijd      number (6,2);
  l_Gem_GAP                    varchar2 (10);
  l_Training_Stress_Score      number (6,2);
  l_Gem_vermogen               number (6,2);
  l_Max_vermogen               number (6,2);
  l_Grit                       number (6,2);
  l_Flow                       number (6,2);
  l_Gemiddelde_Swolf           number (4,2);
  l_Gem_slagsnelheid           number (4,2);
  l_Totaal_herhalingen         number (3);
  l_Totaal_sets                number (3);
  l_Duiktijd                   varchar2 (10);
  l_Min_temp                   number (3,1);
  l_Oppervlakte_interval       varchar2 (10);
  l_Decompressie               varchar2 (10);
  l_Beste_rondetijd            interval day to second;
  l_Aantal_ronden              number (3);
  l_Max_temp                   number (3,1);
  l_Tijd_bewogen               interval day to second;
  l_Verstreken_tijd            interval day to second;
  l_Minimum_hoogte             number (4);
  l_Maximum_hoogte             number (4);
  l_count                      integer (6);
  l_id                         integer;
--
  function next_item return varchar2 
  is 
  begin
    l_string := ltrim (l_string, ',');
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
function to_nr (p_string in varchar2, p_col in varchar2 default null) return number
is 
l_nr    number (20,4);
begin
--  dbms_output.put_line ('to_nr: ' || p_col || '   ' || p_string);
  if p_string = '--' then return null;
  elsif p_string like '%:%' -- tempo 
  then l_nr := round(3600 / (60 * substr (p_string, 1, instr (p_string, ':') - 1) + substr (p_string, instr (p_string, ':') + 1)), 2);  
  else l_nr := to_number (replace (p_string, ',', '.'));  
  end if;
  return l_nr;

exception when others then
  util.show_error ('Not a number: ' || p_col || '  ' || p_string , sqlerrm);
  return null;
end;
--
--
function to_ds (p_string in varchar2, p_col in varchar2 default null) return interval day to second
is 
begin
--  dbms_output.put_line ('to_ds: ' || p_col || '   ' || p_string);
  if instr (p_string, ':', 1, 2) = 0
  then return to_dsinterval ( '00 00:' || replace (p_string, ',', '.'));
  else return to_dsinterval ( '00 ' || replace (p_string, ',', '.'));
  end if;

exception when others then
  util.show_error ('Not an interval: ' || p_col || '  ' || p_string, sqlerrm);
  return null;
end;
begin
  utl_file.fclose_all;
  for f in (select file_name, substr(file_name, instr (file_name, chr(92), 1, 3) +1) fn  from table (get_file_name ('C:\Work\garmin\ ', 'csv')) order by file_name)
  loop 
  begin 
    l_fhandle := utl_file.fopen ('GARMIN' , f.fn, 'r' );
    utl_file.get_line (l_fhandle, l_string);
	l_string_row.delete;
	l_count := 0;
	l_first := next_item;
	loop 
    l_next := next_item;
    exit when l_next is null;
	l_count := l_count + 1;
	l_string_row (l_count) := l_next;
	end loop;
	
  if l_first = 'Activiteittype' and f.fn = 'Activities.csv'
  then 
    insert into garmin garmin (persoon, Activiteittype, Datum) values (p_user, next_item, to_date (next_item, 'YYYY-MM-DD HH24:MI:SS') returning id into l_id;
	  loop
	  begin 
		utl_file.get_line (l_fhandle, l_string);
--		dbms_output.put_line( '--');
 -- dbms_output.put_line(l_string);
		l_Activiteittype                      := next_item;      
		l_Datum                               := to_date (next_item, 'YYYY-MM-DD HH24:MI:SS');       -- ,2023-07-12 14:41:48                	
		l_Favoriet                            := next_item;                     	
		l_Titel                               := next_item;                     	
		l_Afstand                             := to_nr (next_item, 'Afstand');                     	
		l_Calorieen                           := to_nr (next_item, 'Calorieen');                   	
		l_Tijd                                := to_ds  (next_item, 'Tijd');                     	
		l_Gem_HS                              := to_nr (next_item, 'Gem_HS');                   	
		l_Max_HS                              := to_nr (next_item, 'Max_HS');                   	
		l_Aeroob_TE                           := to_nr (next_item, 'Aeroob_TE');          	
		l_Gemiddelde_fietscadans              := next_item;     	
		l_Max_fietscadans                     := next_item;           	
		l_Gemiddelde_snelheid                 := to_nr (next_item, 'Gemiddelde_snelheid');   	
		l_Max_snelheid                        := to_nr (next_item, 'Max_snelheid');         	
		l_Totale_stijging                     := to_nr (next_item, 'Totale_stijging');           	
		l_Totale_daling                       := to_nr (next_item, 'Totale_daling');               	
		l_Gem_staplengte                      := to_nr (next_item, 'Gem_staplengte');           	
		l_Gemiddelde_verticale_ratio	      := to_nr (next_item, 'Gemiddelde_verticale_ratio');     
		l_Gem_verticale_oscillatie  	      := to_nr (next_item, 'Gem_verticale_oscillatie');  
		l_Gem_grondcontacttijd                := to_nr (next_item, 'Gem_grondcontacttijd'); 	
		l_Gem_GAP                             := next_item;             	
		l_Training_Stress_Score               := to_nr (next_item, 'Training_Stress_Score');   	
		l_Gem_vermogen                        := to_nr (next_item, 'Gem_vermogen');           	
		l_Max_vermogen                        := to_nr (next_item, 'Max_vermogen');        	
		l_Grit                                := to_nr (next_item, 'Grit');              	
		l_Flow                                := to_nr (next_item, 'Flow');          	
		l_Gemiddelde_Swolf                    := to_nr (next_item, 'Gemiddelde_Swolf');     	
		l_Gem_slagsnelheid                    := to_nr (next_item, 'Gem_slagsnelheid');       	
		l_Totaal_herhalingen                  := to_nr (next_item, 'Totaal_herhalingen');       	
		l_Totaal_sets                         := to_nr (next_item, 'Totaal_sets');                	
		l_Duiktijd                            := next_item;                  	
		l_Min_temp                            := to_nr (next_item, 'Min_temp');                       	
		l_Oppervlakte_interval                := next_item;           	
		l_Decompressie                        := next_item;           	
		l_Beste_rondetijd                     := to_ds (next_item, 'Beste_rondetijd');          	
		l_Aantal_ronden                       := to_nr (next_item, 'Aantal_ronden');             	
		l_Max_temp                            := to_nr (next_item, 'Max_temp');                	
		l_Tijd_bewogen                        := to_ds (next_item, 'Tijd_bewogen');           	
		l_Verstreken_tijd                     := to_ds (next_item, 'Verstreken_tijd');            	
		l_Minimum_hoogte                      := to_nr (next_item, 'Minimum_hoogte');                  	
		l_Maximum_hoogte                      := to_nr (next_item, 'Maximum_hoogte');	

	  insert into garmin (persoon,Activiteittype,Datum,Favoriet,Titel,Afstand,Calorieen,Tijd,Gem_HS,Max_HS,Aeroob_TE,Gemiddelde_fietscadans,Max_fietscadans,Gemiddelde_snelheid,Max_snelheid,Totale_stijging,
		 Totale_daling,Gem_staplengte,Gemiddelde_verticale_ratio,Gem_verticale_oscillatie,Gem_grondcontacttijd,Gem_GAP,Training_Stress_Score,Gem_vermogen,Max_vermogen,Grit,Flow,Gemiddelde_Swolf,
		 Gem_slagsnelheid,Totaal_herhalingen,Totaal_sets,Duiktijd,Min_temp,Oppervlakte_interval,Decompressie,Beste_rondetijd,Aantal_ronden,Max_temp,Tijd_bewogen,Verstreken_tijd,Minimum_hoogte,Maximum_hoogte)
	  values  (p_user,l_Activiteittype,l_Datum,l_Favoriet,l_Titel,l_Afstand,l_Calorieen,l_Tijd,l_Gem_HS,l_Max_HS,l_Aeroob_TE,l_Gemiddelde_fietscadans,l_Max_fietscadans,l_Gemiddelde_snelheid,l_Max_snelheid,l_Totale_stijging,
		 l_Totale_daling,l_Gem_staplengte,l_Gemiddelde_verticale_ratio,l_Gem_verticale_oscillatie,l_Gem_grondcontacttijd,l_Gem_GAP,l_Training_Stress_Score,l_Gem_vermogen,l_Max_vermogen,l_Grit,l_Flow,l_Gemiddelde_Swolf,
		 l_Gem_slagsnelheid,l_Totaal_herhalingen,l_Totaal_sets,l_Duiktijd,l_Min_temp,l_Oppervlakte_interval,l_Decompressie,l_Beste_rondetijd,l_Aantal_ronden,l_Max_temp,l_Tijd_bewogen,l_Verstreken_tijd,l_Minimum_hoogte,l_Maximum_hoogte);

  
  exception 
  when dup_val_on_index then null;  
  when no_data_found
  then utl_file.fclose (l_fhandle);
    exit;
  end;
  end loop;
  
  elsif l_first = 'Ronden' and f.fn like 'activity%' and 1=2
  then  			dbms_output.put_line('File:  '||  f.fn);
		l_string_row.delete;
		l_count := 1;
		loop
		  begin 
		    l_fhandle := utl_file.fopen ('GARMIN' , f.fn, 'r' );
		    utl_file.get_line (l_fhandle, l_string);
--			dbms_output.put_line(l_string);
	        l_string_row (l_count) := l_string;	  
		  exception 
		  when no_data_found
		  then utl_file.fclose (l_fhandle);
		  exit;
		  end;
		end loop;  
  end if;
  utl_file.fclose_all;
  exception when others then 
    util.show_error ('Error for file: ' || f.fn, sqlerrm);
  end;
  end loop;
commit;
end;
/

exec load_garmin_data ('Theo')


select * from table(get_file_name ('C:\Work\garmin\', 'csv'));

begin
for j in (select substr (file_name, instr (file_name, '\', 1, 3) + 1) file_name
 from table(get_file_name ('C:\Work\Fotos')))
loop
  begin
    blob_pkg.delete_file (j.file_name, 'WORK');
  exception when others then null;
  end loop;
end loop;
end;