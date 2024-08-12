Select * from (
Select *
	,ROW_NUMBER() OVER(PARTITION BY CI_Name ORDER BY Busines_Criticality_Ranking asc) AS Busines_Criticality_and_environment_priority_order
from
(SELECT distinct
	A.sys_id as sys_id,
	A.cmdb_ci__sys_created_on as sys_created_on,
	A.cmdb_ci__sys_updated_on as sys_updated_on,
	A.CI_Name as CI_Name,
	A.CI_ID as CI_ID,
	A.op_team as Op_Team,
	B.cmdb_ci_service__Service as Service,
	C.cmdb_ci_service_discovered_Application__Service as Application_Service,
	C.cmdb_ci_service_discovered__used_for as Environment,
	E.service_offering__Service_Offering as Service_Offering,
	C.cmdb_ci_service_discovered__busines_criticality as Busines_Criticality,
	F.sys_user_group__Assignment_Group as Assignment_Group,
	--G.sys_choice__label as Service_Class,
	H.cmdb_rel_ci_2_parent as Service_Class_Detail,
	H.cmdb_rel_ci_2__parent_service_class as Service_Class,
	C.cmn_location__full_name as Location,
	A.ip_address as Ip_Address,
	C.cmdb_ci_service_discovered__u_ahimc_service_window as Service_Window,
	--B.cmdb_rel_ci_1__parent_service_class as application_service_service_classe,
	H.cmdb_rel_ci_2__child_op_team AS Application_Service_op_team,
	A.cmdb_ci__sys_class_name as cmdb_ci__sys_class_name,
	C.cmdb_ci_service_discovered__busines_criticality_ranking_order as Busines_Criticality_Ranking
	
FROM (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.sys_created_on AS cmdb_ci__sys_created_on,
		cmdb_ci.sys_updated_on AS cmdb_ci__sys_updated_on,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_ci.u_hrd_c_op_team AS op_team,
		cmdb_ci.ip_address AS ip_address,
		cmdb_ci.u_service_level,
		cmdb_ci.sys_class_name AS cmdb_ci__sys_class_name
		
	FROM cmdb_ci AS cmdb_ci
	WHERE 1=1
	AND cmdb_ci.u_division IN ('Airbus Helicopters','af5abc8cdb28f3c08838453f29961989') /* Right now we are only considering only aibus helicopter data (AH) */
	AND cmdb_ci.u_hrd_c_op_team ='CoreOperate' /* Right now we are only considering only CoreOperate data */
	AND cmdb_ci.install_status <> '7' /*  NOT Retired */
 	AND cmdb_ci.sys_class_name <> 'cmdb_ci_service_discovered' /*  NOT Application */
	--AND cmdb_ci.sys_updated_on  BETWEEN CAST('2024-04-01' AS DATE) AND CAST('2024-06-01' AS DATE)/* Right now we are only getting data for limited days */
) AS A
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_rel_ci_1.child AS cmdb_rel_ci_1__child, -- this is to check the join working or not, should be commented and removed later
		cmdb_rel_ci_1.type AS cmdb_rel_ci_1__type,
		cmdb_rel_ci_1.sys_created_on AS cmdb_rel_ci_1__sys_created_on,
		cmdb_rel_ci_1.sys_updated_on AS cmdb_rel_ci_1__sys_updated_on,
		cmdb_rel_ci_1.parent AS cmdb_rel_ci_1__parent,
   (case cmdb_rel_ci_1.parent
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Silver_12/5_CoreOperate' Then 'Silver'
   else '--'
		END) AS cmdb_rel_ci_1__parent_service_class,
		service_offering.parent AS service_offering__parent,
		service_offering.sys_created_on AS service_offering__sys_created_on,
		service_offering.sys_updated_on AS service_offering__sys_updated_on,
		cmdb_ci_service.name AS cmdb_ci_service__Service, /* Service Offering Parent */
		cmdb_ci_service.sys_created_on AS cmdb_ci_service__sys_created_on,
		cmdb_ci_service.sys_updated_on AS cmdb_ci_service__sys_updated_on
	
	FROM cmdb_ci AS cmdb_ci
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN service_offering  AS service_offering ON (service_offering.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN cmdb_ci_service AS cmdb_ci_service ON (cmdb_ci_service.sys_id = service_offering.parent)
	
	--WHERE cmdb_ci_service.name is NOT NULL
	/*#AND cmdb_ci_service.name IN ('Service Management OS4','Service Management AHSAD') */ /*# Right now we are only considering two services, there sys_id are 'd3602424dbb0c81041fafc9b0c961987','db602424dbb0c81041fafc9b0c961985' */
) AS B ON (A.sys_id = B.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_ci_service_discovered.name AS cmdb_ci_service_discovered_Application__Service,
		cmdb_ci_service_discovered.u_hrd_c_op_team AS cmdb_ci_service_discovered__op_team,
		cmdb_ci_service_discovered.sys_class_name AS cmdb_ci_service_discovered__class_name,
		cmdb_ci_service_discovered.used_for AS cmdb_ci_service_discovered__used_for,
		(case concat(cmdb_ci_service_discovered.used_for, concat(' + ', cmdb_ci_service_discovered.busines_criticality))
			when 'Production + 1 - most critica' then '01'
			when 'Production + 2 - somewhat cri' then '02'
			when 'Production + 3 - less critica' then '03'
			when 'Production + 4 - not critical' then '04'
			when 'Development + 1 - most critica' then '05'
			when 'Development + 2 - somewhat cri' then '06'
			when 'Development + 3 - less critica' then '07'
			when 'Development + 4 - not critical' then '08'
			when 'Validation + 1 - most critica' then '09'
			when 'Validation + 2 - somewhat cri' then '10'
			when 'Validation + 3 - less critica' then '11'
			when 'Validation + 4 - not critical' then '12'
			when 'Pre-production + 1 - most critica' then '13'
			when 'Pre-production + 2 - somewhat cri' then '14'
			when 'Pre-production + 3 - less critica' then '15'
			when 'Pre-production + 4 - not critical' then '16'
			when 'Pre-Production + 1 - most critica' then '13'
			when 'Pre-Production + 2 - somewhat cri' then '14'
			when 'Pre-Production + 3 - less critica' then '15'
			when 'Pre-Production + 4 - not critical' then '16'
			else '29'
		END) AS cmdb_ci_service_discovered__busines_criticality_ranking_order,
		cmdb_ci_service_discovered.busines_criticality AS cmdb_ci_service_discovered__busines_criticality,
		cmdb_ci_service_discovered.u_ahimc_service_window AS cmdb_ci_service_discovered__u_ahimc_service_window,
		cmdb_rel_ci_1.type AS cmdb_rel_ci_1__type, /* gaurav_remark - This join need to be checked again, need to create a seperate join for this as its comming from different table */
		cmdb_rel_type.name AS cmdb_rel_type__type_name, /* gaurav_remark - This join need to be checked again, need to create a seperate join for this as its comming from different table */
		cmn_location.full_name AS cmn_location__full_name
		
	FROM cmdb_ci AS cmdb_ci
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN cmdb_ci_service_discovered AS cmdb_ci_service_discovered ON (cmdb_ci_service_discovered.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN cmdb_rel_type AS cmdb_rel_type ON(cmdb_rel_type.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_type_sys_id)
	LEFT JOIN cmn_location as cmn_location ON (cmn_location.sys_id = cmdb_ci.location)
	
	--WHERE cmdb_ci_service_discovered.name is NOT NULL
) AS C ON (A.sys_id = C.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_ci_server.host_name AS cmdb_ci_server__host_name
	FROM cmdb_ci AS cmdb_ci
	
	LEFT JOIN cmdb_ci_server AS cmdb_ci_server  ON(cmdb_ci_server.sys_id = cmdb_ci.sys_id)
	
	--WHERE cmdb_ci_server.host_name is NOT NULL
) AS D ON (A.sys_id = D.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		service_offering.name AS service_offering__Service_Offering,
		(case service_offering.name
			when 'AHSAD Gold 24h/7d' then 'Gold'
			when 'AHSAD Gold 12h/5d' then 'Gold'
			when 'AHSAD Bronze' then 'Bronze'
			when 'AHSAD Silver' then 'Silver'
			when 'AHSAD Platinum 24h/7d' then 'Platinum'
			when 'AHSAD Platinum 12h/5d' then 'Platinum'
			when 'AHSAD Datacenter Service 10h/5d' then 'Datacenter Service'
		else service_offering.name
		END) AS service_offering__Service_Level,
		service_offering.busines_criticality AS service_offering__Business_criticality,
		(case service_offering.busines_criticality
			when '2 - somewhat critical' then 'High'
			when '4 - not critical' then 'Low'
			when '1 - most critical' then 'Critical'
			when '3 - less critical' then 'Standard'
		else service_offering.busines_criticality
		END) AS service_offering__dv_Business_criticality,
		service_offering.used_for AS service_offering__used_for
	FROM cmdb_ci AS cmdb_ci
	
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN service_offering AS service_offering ON (service_offering.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	
	--WHERE service_offering.name is NOT NULL
) AS E ON (A.sys_id = E.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		sys_user_group.name AS sys_user_group__Assignment_Group
	FROM cmdb_ci AS cmdb_ci
	
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN cmdb_rel_group AS cmdb_rel_group ON(cmdb_rel_group.ci = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN sys_user_group AS sys_user_group  ON(sys_user_group.sys_id = cmdb_rel_group.group)
	
	--WHERE sys_user_group.name is NOT NULL
) AS F ON (A.sys_id = F.sys_id)
LEFT JOIN (
	SELECT
		sys_choice.value as sys_choice__value,
		sys_choice.label as sys_choice__label
	FROM sys_choice
	WHERE sys_choice.Table ='cmdb_ci'
	AND sys_choice.element='u_service_level'
) AS G ON (A.u_service_level = G.sys_choice__value)
LEFT JOIN (
	select distinct
		cmdb_ci.sys_id as cmdb_ci_sys_id,
		cmdb_ci.name as cmdb_ci_name,
		cmdb_rel_ci_2.child_op_team AS cmdb_rel_ci_2__child_op_team,
		cmdb_rel_ci_2.parent as cmdb_rel_ci_2_parent,
			(case cmdb_rel_ci_2.parent
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Silver_12/5_CoreOperate' Then 'Silver'
			else '--'
			END) AS cmdb_rel_ci_2__parent_service_class
	from cmdb_ci
	LEFT JOIN cmdb_rel_ci as cmdb_rel_ci_1 on (cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN cmdb_ci_service_discovered AS cmdb_ci_service_discovered ON (cmdb_ci_service_discovered.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN cmdb_rel_ci as cmdb_rel_ci_2 on (cmdb_rel_ci_2.cmdb_rel_ci_child_sys_id = cmdb_ci_service_discovered.sys_id)
	where cmdb_rel_ci_2.parent LIKE '%Application_AAAS%'
) AS H ON (A.sys_id = H.cmdb_ci_sys_id)
) AS raw_data
WHERE 1=1
and raw_data.Service_Offering NOT LIKE '%[DO NOT USE - FOR FUTURE]%'
and raw_data.Service_Offering LIKE '%OS4%'
or raw_data.Service_Offering LIKE '%AHSAD%'
ORDER BY
	raw_data.CI_Name
	--,raw_data.Busines_Criticality
	,raw_data.Busines_Criticality_Ranking
) AS curated_data
where 1=1
--and curated_data.cmdb_rel_ci_parent LIKE '%[DO NOT USE - FOR FUTURE]%'
--and curated_data.cmdb_rel_ci_parent LIKE '%Application_AAAS%'
--and curated_data.busines_criticality_priority_order = 1 -- this condition is obsolete now, going to use condition with this field "Busines_Criticality_and_environment_priority_order"
and curated_data.Busines_Criticality_and_environment_priority_order = 1
Sql Query Passed Final
Select * From view_0BC7_cmdb_ci limit 10;
Testing Print Remark - Data Reading Redshift - Creating Variable
Testing Print Remark - Data Reading Redshift - Creating Query
Select * from (
Select *
	,ROW_NUMBER() OVER(PARTITION BY CI_Name ORDER BY Busines_Criticality_Ranking asc) AS Busines_Criticality_and_environment_priority_order
from
(SELECT distinct
	A.sys_id as sys_id,
	A.cmdb_ci__sys_created_on as sys_created_on,
	A.cmdb_ci__sys_updated_on as sys_updated_on,
	A.CI_Name as CI_Name,
	A.CI_ID as CI_ID,
	A.op_team as Op_Team,
	B.cmdb_ci_service__Service as Service,
	C.cmdb_ci_service_discovered_Application__Service as Application_Service,
	C.cmdb_ci_service_discovered__used_for as Environment,
	E.service_offering__Service_Offering as Service_Offering,
	C.cmdb_ci_service_discovered__busines_criticality as Busines_Criticality,
	F.sys_user_group__Assignment_Group as Assignment_Group,
	--G.sys_choice__label as Service_Class,
	H.cmdb_rel_ci_2_parent as Service_Class_Detail,
	H.cmdb_rel_ci_2__parent_service_class as Service_Class,
	C.cmn_location__full_name as Location,
	A.ip_address as Ip_Address,
	C.cmdb_ci_service_discovered__u_ahimc_service_window as Service_Window,
	--B.cmdb_rel_ci_1__parent_service_class as application_service_service_classe,
	H.cmdb_rel_ci_2__child_op_team AS Application_Service_op_team,
	A.cmdb_ci__sys_class_name as cmdb_ci__sys_class_name,
	C.cmdb_ci_service_discovered__busines_criticality_ranking_order as Busines_Criticality_Ranking
	
FROM (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.sys_created_on AS cmdb_ci__sys_created_on,
		cmdb_ci.sys_updated_on AS cmdb_ci__sys_updated_on,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_ci.u_hrd_c_op_team AS op_team,
		cmdb_ci.ip_address AS ip_address,
		cmdb_ci.u_service_level,
		cmdb_ci.sys_class_name AS cmdb_ci__sys_class_name
		
	FROM cmdb_ci AS cmdb_ci
	WHERE 1=1
	AND cmdb_ci.u_division IN ('Airbus Helicopters','af5abc8cdb28f3c08838453f29961989') /* Right now we are only considering only aibus helicopter data (AH) */
	AND cmdb_ci.u_hrd_c_op_team ='CoreOperate' /* Right now we are only considering only CoreOperate data */
	AND cmdb_ci.install_status <> '7' /*  NOT Retired */
 	AND cmdb_ci.sys_class_name <> 'cmdb_ci_service_discovered' /*  NOT Application */
	--AND cmdb_ci.sys_updated_on  BETWEEN CAST('2024-04-01' AS DATE) AND CAST('2024-06-01' AS DATE)/* Right now we are only getting data for limited days */
) AS A
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_rel_ci_1.child AS cmdb_rel_ci_1__child, -- this is to check the join working or not, should be commented and removed later
		cmdb_rel_ci_1.type AS cmdb_rel_ci_1__type,
		cmdb_rel_ci_1.sys_created_on AS cmdb_rel_ci_1__sys_created_on,
		cmdb_rel_ci_1.sys_updated_on AS cmdb_rel_ci_1__sys_updated_on,
		cmdb_rel_ci_1.parent AS cmdb_rel_ci_1__parent,
   (case cmdb_rel_ci_1.parent
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
        When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Silver_12/5_CoreOperate' Then 'Silver'
   else '--'
		END) AS cmdb_rel_ci_1__parent_service_class,
		service_offering.parent AS service_offering__parent,
		service_offering.sys_created_on AS service_offering__sys_created_on,
		service_offering.sys_updated_on AS service_offering__sys_updated_on,
		cmdb_ci_service.name AS cmdb_ci_service__Service, /* Service Offering Parent */
		cmdb_ci_service.sys_created_on AS cmdb_ci_service__sys_created_on,
		cmdb_ci_service.sys_updated_on AS cmdb_ci_service__sys_updated_on
	
	FROM cmdb_ci AS cmdb_ci
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN service_offering  AS service_offering ON (service_offering.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN cmdb_ci_service AS cmdb_ci_service ON (cmdb_ci_service.sys_id = service_offering.parent)
	
	--WHERE cmdb_ci_service.name is NOT NULL
	/*#AND cmdb_ci_service.name IN ('Service Management OS4','Service Management AHSAD') */ /*# Right now we are only considering two services, there sys_id are 'd3602424dbb0c81041fafc9b0c961987','db602424dbb0c81041fafc9b0c961985' */
) AS B ON (A.sys_id = B.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_ci_service_discovered.name AS cmdb_ci_service_discovered_Application__Service,
		cmdb_ci_service_discovered.u_hrd_c_op_team AS cmdb_ci_service_discovered__op_team,
		cmdb_ci_service_discovered.sys_class_name AS cmdb_ci_service_discovered__class_name,
		cmdb_ci_service_discovered.used_for AS cmdb_ci_service_discovered__used_for,
		(case concat(cmdb_ci_service_discovered.used_for, concat(' + ', cmdb_ci_service_discovered.busines_criticality))
			when 'Production + 1 - most critica' then '01'
			when 'Production + 2 - somewhat cri' then '02'
			when 'Production + 3 - less critica' then '03'
			when 'Production + 4 - not critical' then '04'
			when 'Development + 1 - most critica' then '05'
			when 'Development + 2 - somewhat cri' then '06'
			when 'Development + 3 - less critica' then '07'
			when 'Development + 4 - not critical' then '08'
			when 'Validation + 1 - most critica' then '09'
			when 'Validation + 2 - somewhat cri' then '10'
			when 'Validation + 3 - less critica' then '11'
			when 'Validation + 4 - not critical' then '12'
			when 'Pre-production + 1 - most critica' then '13'
			when 'Pre-production + 2 - somewhat cri' then '14'
			when 'Pre-production + 3 - less critica' then '15'
			when 'Pre-production + 4 - not critical' then '16'
			when 'Pre-Production + 1 - most critica' then '13'
			when 'Pre-Production + 2 - somewhat cri' then '14'
			when 'Pre-Production + 3 - less critica' then '15'
			when 'Pre-Production + 4 - not critical' then '16'
			else '29'
		END) AS cmdb_ci_service_discovered__busines_criticality_ranking_order,
		cmdb_ci_service_discovered.busines_criticality AS cmdb_ci_service_discovered__busines_criticality,
		cmdb_ci_service_discovered.u_ahimc_service_window AS cmdb_ci_service_discovered__u_ahimc_service_window,
		cmdb_rel_ci_1.type AS cmdb_rel_ci_1__type, /* gaurav_remark - This join need to be checked again, need to create a seperate join for this as its comming from different table */
		cmdb_rel_type.name AS cmdb_rel_type__type_name, /* gaurav_remark - This join need to be checked again, need to create a seperate join for this as its comming from different table */
		cmn_location.full_name AS cmn_location__full_name
		
	FROM cmdb_ci AS cmdb_ci
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN cmdb_ci_service_discovered AS cmdb_ci_service_discovered ON (cmdb_ci_service_discovered.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN cmdb_rel_type AS cmdb_rel_type ON(cmdb_rel_type.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_type_sys_id)
	LEFT JOIN cmn_location as cmn_location ON (cmn_location.sys_id = cmdb_ci.location)
	
	--WHERE cmdb_ci_service_discovered.name is NOT NULL
) AS C ON (A.sys_id = C.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		cmdb_ci_server.host_name AS cmdb_ci_server__host_name
	FROM cmdb_ci AS cmdb_ci
	
	LEFT JOIN cmdb_ci_server AS cmdb_ci_server  ON(cmdb_ci_server.sys_id = cmdb_ci.sys_id)
	
	--WHERE cmdb_ci_server.host_name is NOT NULL
) AS D ON (A.sys_id = D.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		service_offering.name AS service_offering__Service_Offering,
		(case service_offering.name
			when 'AHSAD Gold 24h/7d' then 'Gold'
			when 'AHSAD Gold 12h/5d' then 'Gold'
			when 'AHSAD Bronze' then 'Bronze'
			when 'AHSAD Silver' then 'Silver'
			when 'AHSAD Platinum 24h/7d' then 'Platinum'
			when 'AHSAD Platinum 12h/5d' then 'Platinum'
			when 'AHSAD Datacenter Service 10h/5d' then 'Datacenter Service'
		else service_offering.name
		END) AS service_offering__Service_Level,
		service_offering.busines_criticality AS service_offering__Business_criticality,
		(case service_offering.busines_criticality
			when '2 - somewhat critical' then 'High'
			when '4 - not critical' then 'Low'
			when '1 - most critical' then 'Critical'
			when '3 - less critical' then 'Standard'
		else service_offering.busines_criticality
		END) AS service_offering__dv_Business_criticality,
		service_offering.used_for AS service_offering__used_for
	FROM cmdb_ci AS cmdb_ci
	
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN service_offering AS service_offering ON (service_offering.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	
	--WHERE service_offering.name is NOT NULL
) AS E ON (A.sys_id = E.sys_id)
LEFT JOIN (
	SELECT
		cmdb_ci.sys_id AS sys_id,
		cmdb_ci.name AS CI_Name,
		cmdb_ci.u_id AS CI_ID,
		sys_user_group.name AS sys_user_group__Assignment_Group
	FROM cmdb_ci AS cmdb_ci
	
	LEFT JOIN cmdb_rel_ci AS cmdb_rel_ci_1 ON(cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN cmdb_rel_group AS cmdb_rel_group ON(cmdb_rel_group.ci = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN sys_user_group AS sys_user_group  ON(sys_user_group.sys_id = cmdb_rel_group.group)
	
	--WHERE sys_user_group.name is NOT NULL
) AS F ON (A.sys_id = F.sys_id)
LEFT JOIN (
	SELECT
		sys_choice.value as sys_choice__value,
		sys_choice.label as sys_choice__label
	FROM sys_choice
	WHERE sys_choice.Table ='cmdb_ci'
	AND sys_choice.element='u_service_level'
) AS G ON (A.u_service_level = G.sys_choice__value)
LEFT JOIN (
	select distinct
		cmdb_ci.sys_id as cmdb_ci_sys_id,
		cmdb_ci.name as cmdb_ci_name,
		cmdb_rel_ci_2.child_op_team AS cmdb_rel_ci_2__child_op_team,
		cmdb_rel_ci_2.parent as cmdb_rel_ci_2_parent,
			(case cmdb_rel_ci_2.parent
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Silver_12/5_CoreOperate' Then 'Silver'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Bronze_12/5_CoreOperate' Then 'Bronze'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Gold_24/7_CoreOperate' Then 'Gold'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AH_CORECOUNTRY_Platinum_24/7_CoreOperate' Then 'Platinum'
				 When '[DO NOT USE - FOR FUTURE] Application_AAAS_AC_Silver_12/5_CoreOperate' Then 'Silver'
			else '--'
			END) AS cmdb_rel_ci_2__parent_service_class
	from cmdb_ci
	LEFT JOIN cmdb_rel_ci as cmdb_rel_ci_1 on (cmdb_rel_ci_1.cmdb_rel_ci_child_sys_id = cmdb_ci.sys_id)
	LEFT JOIN cmdb_ci_service_discovered AS cmdb_ci_service_discovered ON (cmdb_ci_service_discovered.sys_id = cmdb_rel_ci_1.cmdb_rel_ci_parent_sys_id)
	LEFT JOIN cmdb_rel_ci as cmdb_rel_ci_2 on (cmdb_rel_ci_2.cmdb_rel_ci_child_sys_id = cmdb_ci_service_discovered.sys_id)
	where cmdb_rel_ci_2.parent LIKE '%Application_AAAS%'
) AS H ON (A.sys_id = H.cmdb_ci_sys_id)
) AS raw_data
WHERE 1=1
and raw_data.Service_Offering NOT LIKE '%[DO NOT USE - FOR FUTURE]%'
and raw_data.Service_Offering LIKE '%OS4%'
or raw_data.Service_Offering LIKE '%AHSAD%'
ORDER BY
	raw_data.CI_Name
	--,raw_data.Busines_Criticality
	,raw_data.Busines_Criticality_Ranking
) AS curated_data
where 1=1
--and curated_data.cmdb_rel_ci_parent LIKE '%[DO NOT USE - FOR FUTURE]%'
--and curated_data.cmdb_rel_ci_parent LIKE '%Application_AAAS%'
--and curated_data.busines_criticality_priority_order = 1 -- this condition is obsolete now, going to use condition with this field "Busines_Criticality_and_environment_priority_order"
and curated_data.Busines_Criticality_and_environment_priority_order = 1
Testing Print Remark - Data Reading Redshift - Data Reading and Creating DynamicFrame
