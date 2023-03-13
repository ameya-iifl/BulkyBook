﻿/*
*************************************************************************************************      
Created By: Ravindra Sapkal			   Create on:  8 Jan 2019
Brief description:- Upload CKYC Image data of HFC
------------------------------------------------------------------------------------------------      
  To Add/Modify       
------------------------------------------------------------------------------------------------      
Tables Accessed:        
Parameters:-      
 Input:      
 Output:      
 Query to execute Procedure - EXEC Proc_DailyLOSCKYCImageData_CoApplicantHFC '20170401', '20181031', 'N' 

 Change History:-      
 Change# Changed By		Changed on		Reason    
 #001	Ravindra Sapkal	27-Sep-2019		Upload the LOS CKYC data
 #002	Ravindra Sapkal	 8-Jan-2020	    Date format change for the 
 #003	Ravindra Sapkal	 9-Sep-2020	    CKYC Regulatory change for the clients
 #004	Ravindra Sapkal	 30-Apr-2021	CKYC Regulatory change for the clients
 #005	Ravindra Sapkal	 11-May-2021	Update the address 
*************************************************************************************************
*/
CREATE PROCEDURE [dbo].[Proc_DailyLOSCKYCImageData_CoApplicantHFC]
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @FROMDATE DATE = GETDATE() - 15
	DECLARE @TODATE DATE = GETDATE()
	--,@GetImageData CHAR(1)='N'
	DECLARE @BatchId INT = 0
		,@Product CHAR(3) = 'HFC'

	CREATE TABLE #temp (
		STATUS VARCHAR(10)
		,BatchId INT
		)

	INSERT INTO #temp (
		STATUS
		,BatchId
		)
	EXECUTE InsertUpdateBatchDetails @Product
		,0
		,'sys'
		,0
		,0
		,0

	SELECT @BatchId = BatchId
	FROM #temp
	WHERE STATUS = 0

	--declare @prospectno varchar(10)='IL0000109'
	--Declare @FromDate varchar(8)='20170401'
	--Declare @ToDate varchar(8)='20181130'
	IF OBJECT_ID('tempdb..#CustomerIntegrationStatusData') IS NOT NULL
		DROP TABLE #CustomerIntegrationStatusData

	IF OBJECT_ID('tempdb..#POIPOA') IS NOT NULL
		DROP TABLE #POIPOA

	IF OBJECT_ID('tempdb..#APPLICANTIMAGE') IS NOT NULL
		DROP TABLE #APPLICANTIMAGE

	IF OBJECT_ID('tempdb..#temp_Attachments') IS NOT NULL
		DROP TABLE #temp_Attachments;

	IF OBJECT_ID('tempdb..#CTEDOCUMENT_POA') IS NOT NULL
		DROP TABLE #CTEDOCUMENT_POA;

	IF OBJECT_ID('tempdb..#CTEDOCUMENTS_POI') IS NOT NULL
		DROP TABLE #CTEDOCUMENTS_POI;

	IF OBJECT_ID('tempdb..#APPLICANT') IS NOT NULL
		DROP TABLE #APPLICANT;

	--================================================================POA==============================================================		
	SELECT DISTINCT CUID COLLATE SQL_Latin1_General_CP1_CI_AS AS CUID
	INTO #UploadData
	FROM (
		SELECT FromSourceSystemCustomerCode AS CUID
		FROM [Darwin.iifl.in].TrackWizz.dbo.CKYCOUTWARDVIEW WITH (NOLOCK)
		WHERE FromSourceSystem = 'HFC'
		
		UNION
		
		SELECT DISTINCT SourceSystemCustomerCode AS CUID
		FROM [Darwin.iifl.in].TrackWizz.dbo.[CoreCRMCustomerHistory] WITH (NOLOCK)
		WHERE SourceSystem = 'HFC'
			AND RejectionCodes IS NULL
		) AS A

	--================================================================POA==============================================================		
	--DROP TABLE #DAILY_PROSPECTNO
	SELECT DISTINCT T.ProspectNo
		,IORG.cuid
	INTO #DAILY_PROSPECTNO
	FROM (
		SELECT DISTINCT LD_PROSPECTNO AS PROSPECTNO
		FROM IILHFC.dbo.TBL_LEDGERDISBAMT A WITH (NOLOCK)
		INNER JOIN IILHFC.dbo.tbl_clientMaster B WITH (NOLOCK) ON A.LD_PROSPECTNO = B.PROSPECTNO
		WHERE B.ClientStatus = 'ACT'
			AND B.IsTu = 0
		GROUP BY LD_PROSPECTNO
		HAVING MIN(LD_DT) BETWEEN @FROMDATE
				AND @TODATE
		) AS T
	INNER JOIN IILHFC..Indv_OrgMaster IORG WITH (NOLOCK) ON T.ProspectNo = IORG.ProspectNo
	WHERE IORG.ApplicantType = 'COBORROWER'

	--==============Applicant Image====================================================================================================
	SELECT *
	INTO #APPLICANTIMAGE
	FROM (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY IORG.CUID ORDER BY IORG.CUID
					,DV.SRNO DESC
				) AS RN
			,DV.IMAGENAME
			,DV.SRNO
			,DV.APPLICANTTYPE
			,DV.APPLICANTNAME
			,IORG.PANNO
			,DV.MKRID
			,DV.IMAGEPATH AS FILEPATH
			,CASE 
				WHEN CHARINDEX('.', IMAGEPATH) > 0
					THEN '.' + RIGHT(IMAGEPATH, CHARINDEX('.', REVERSE(IMAGEPATH)) - 1)
				ELSE ''
				END AS FILEEXTENSION
			,CASE 
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PAN%'
					THEN 'PANCARD'
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%AADHAAR%'
					THEN 'AADHARCARD'
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PASSPORT%'
					THEN 'PASSPORT'
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%DRIVING%'
					THEN 'DRIVINGLICENCE'
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%VOTER ID%'
					THEN 'VOTERID'
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%UTILITY BILLS%'
					THEN 'UTILITYBILL2M'
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%RENT AGREEMENT%'
					THEN 'EMPLOYERHOUSELETTER'
				WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PHOTO%'
					THEN 'APPLICANTPHOTO'
				ELSE 'OTHERSPOACKYCIND'
				END AS ATTACHMENTCODE
			,DV.PROSPECTNO
			,IORG.CUID AS SOURCESYSYTEMCUSTOMERCODE
			,'' AS CLIENTIMAGE
			,'N' AS PHOTOUPLOADED
			,GETDATE() AS ADDEDON
			,DV.IMAGETYPE
			,IM.DrivingLicense
			,IM.DLExpiry
			,IM.Passport
			,CASE 
				WHEN len(rtrim(ltrim(IM.ppValidity))) = 10
					THEN right(REPLACE(IM.ppValidity, '/', ''), 4) + substring(REPLACE(IM.ppValidity, '/', ''), 3, 2) + substring(REPLACE(IM.ppValidity, '/', ''), 1, 2)
				ELSE IM.ppValidity
				END AS ppValidity
			,IM.Adharno
			,IM.VotersID
		FROM IILHFC.DBO.TBL_ICMDOCUMENTVALIDATE DV WITH (NOLOCK)
		INNER JOIN #DAILY_PROSPECTNO DP ON DV.PROSPECTNO = DP.PROSPECTNO
		INNER JOIN IILHFC..INDV_ORGMASTER IORG WITH (NOLOCK) ON IORG.PROSPECTNO = DV.PROSPECTNO
			AND IORG.APPLICANTTYPE = DV.APPLICANTTYPE
			AND IORG.NAME = DV.APPLICANTNAME
		INNER JOIN IILHFC.DBO.INDIVIDUAL_MASTER IM WITH (NOLOCK) ON IORG.PROSPECTNO = IM.IND_PROSPECTNO
			AND DV.APPLICANTTYPE <> 'APPLICANT'
			AND REPLACE(DV.APPLICANTNAME, ' ', '') = REPLACE(RTRIM(LTRIM(IM.FNAME)) + ' ' + CASE 
					WHEN ISNULL(RTRIM(LTRIM(IM.MNAME)), '') = ''
						THEN ''
					ELSE RTRIM(LTRIM(IM.MNAME)) + ' '
					END + ISNULL(RTRIM(LTRIM(IM.LNAME)), ''), ' ', '')
		INNER JOIN IILHFC..TBL_TU_DOCUMENT_MASTER TD WITH (NOLOCK) ON TD.CATEGORY_ID = DV.CATEGORY_ID
			AND TD.ID = DV.DOC_ID
		LEFT JOIN IILHFC.DBO.TBL_CLIENTMASTER CM WITH (NOLOCK) ON CM.PROSPECTNO = DV.PROSPECTNO
		WHERE CM.ISTU = 0
			AND DV.IMAGETYPE = 'APPLICANTPHOTO'
			AND DV.APPLICANTTYPE = 'COBORROWER'
		) AS T
	WHERE RN = 1

	--================Applicant POA====================================================================================================
	SELECT *
	INTO #CTEDOCUMENTS_POA
	FROM (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY SOURCESYSYTEMCUSTOMERCODE ORDER BY SOURCESYSYTEMCUSTOMERCODE
					,SRNO DESC
				) AS RN
			,*
		FROM (
			SELECT DV.IMAGENAME
				,DV.SRNO
				,DV.APPLICANTTYPE
				,DV.APPLICANTNAME
				,IORG.PANNO
				,DV.MKRID
				,DV.IMAGEPATH AS FILEPATH
				,CASE 
					WHEN CHARINDEX('.', IMAGEPATH) > 0
						THEN '.' + RIGHT(IMAGEPATH, CHARINDEX('.', REVERSE(IMAGEPATH)) - 1)
					ELSE ''
					END AS FILEEXTENSION
				,CASE 
					--WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PAN%'
					--	THEN 'PANCARD'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%AADHAAR%'
						THEN 'AadharCard'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%ekyc authentication%' -- ADDED BY RAJU JADHAV ON 28TH JUNE 2022
						THEN 'AadharCard'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PASSPORT%'
						THEN 'Passport'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%DRIVING%'
						THEN 'DRIVINGLICENCE'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%VOTER ID%'
						THEN 'VOTERID'
					ELSE 'OTHERSPOACKYCIND'
					END AS ATTACHMENTCODE
				,DV.PROSPECTNO
				,IORG.CUID AS SOURCESYSYTEMCUSTOMERCODE
				,'' AS CLIENTIMAGE
				,'N' AS PHOTOUPLOADED
				,GETDATE() AS ADDEDON
				,DV.IMAGETYPE
			FROM IILHFC.DBO.TBL_ICMDOCUMENTVALIDATE DV WITH (NOLOCK)
			INNER JOIN #APPLICANTIMAGE DP ON DV.PROSPECTNO = DP.PROSPECTNO
			INNER JOIN IILHFC..INDV_ORGMASTER IORG WITH (NOLOCK) ON IORG.PROSPECTNO = DV.PROSPECTNO
				AND IORG.APPLICANTTYPE = DV.APPLICANTTYPE
				AND IORG.NAME = DV.APPLICANTNAME
			INNER JOIN IILHFC.DBO.INDIVIDUAL_MASTER IM WITH (NOLOCK) ON IORG.PROSPECTNO = IM.IND_PROSPECTNO
				AND DV.APPLICANTTYPE <> 'APPLICANT'
				AND REPLACE(DV.APPLICANTNAME, ' ', '') = REPLACE(RTRIM(LTRIM(IM.FNAME)) + ' ' + CASE 
						WHEN ISNULL(RTRIM(LTRIM(IM.MNAME)), '') = ''
							THEN ''
						ELSE RTRIM(LTRIM(IM.MNAME)) + ' '
						END + ISNULL(RTRIM(LTRIM(IM.LNAME)), ''), ' ', '')
			INNER JOIN IILHFC..TBL_TU_DOCUMENT_MASTER TD WITH (NOLOCK) ON TD.CATEGORY_ID = DV.CATEGORY_ID
				AND TD.ID = DV.DOC_ID
			LEFT JOIN IILHFC.DBO.TBL_CLIENTMASTER CM WITH (NOLOCK) ON CM.PROSPECTNO = DV.PROSPECTNO
			WHERE CM.ISTU = 0
				AND DV.IMAGETYPE = 'LOS POA'
				AND IORG.APPLICANTTYPE = 'COBORROWER'
				AND (
					LOWER(TD.DOCUMENT_NAME) LIKE '%DRIVING%'
					OR LOWER(TD.DOCUMENT_NAME) LIKE '%AADHAAR%'
					OR LOWER(TD.DOCUMENT_NAME) LIKE '%VOTER ID%'
					OR LOWER(TD.DOCUMENT_NAME) LIKE '%PASSPORT%'
					)
				OR LOWER(TD.DOCUMENT_NAME) LIKE '%ekyc authentication%' -- ADDED BY RAJU JADHAV ON 28TH JULY 2022
			
			UNION ALL
			
			SELECT DV.IMAGENAME
				,DV.SRNO
				,DV.APPLICANTTYPE
				,DV.APPLICANTNAME
				,IORG.PANNO
				,DV.MKRID
				,DV.IMAGEPATH AS FILEPATH
				,CASE 
					WHEN CHARINDEX('.', IMAGEPATH) > 0
						THEN '.' + RIGHT(IMAGEPATH, CHARINDEX('.', REVERSE(IMAGEPATH)) - 1)
					ELSE ''
					END AS FILEEXTENSION
				,CASE 
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%AADHAAR%'
						THEN 'AADHARCARD'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%ekyc authentication%' -- ADDED BY RAJU JADHAV ON 28TH JUNE 2022
						THEN 'AadharCard'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PASSPORT%'
						THEN 'PASSPORT'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%DRIVING%'
						THEN 'DRIVINGLICENCE'
					WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%VOTER ID%'
						THEN 'VOTERID'
					ELSE 'OTHERSPOACKYCIND'
					END AS ATTACHMENTCODE
				,DV.PROSPECTNO
				,IORG.CUID AS SOURCESYSYTEMCUSTOMERCODE
				,'' AS CLIENTIMAGE
				,'N' AS PHOTOUPLOADED
				,GETDATE() AS ADDEDON
				,DV.IMAGETYPE
			FROM IILHFC.DBO.TBL_ICMDOCUMENTVALIDATE DV WITH (NOLOCK)
			INNER JOIN #APPLICANTIMAGE DP ON DV.PROSPECTNO = DP.PROSPECTNO
			INNER JOIN IILHFC..INDV_ORGMASTER IORG WITH (NOLOCK) ON IORG.PROSPECTNO = DV.PROSPECTNO
				AND IORG.APPLICANTTYPE = DV.APPLICANTTYPE
				AND IORG.NAME = DV.APPLICANTNAME
			INNER JOIN IILHFC.DBO.INDIVIDUAL_MASTER IM WITH (NOLOCK) ON IORG.PROSPECTNO = IM.IND_PROSPECTNO
				AND DV.APPLICANTTYPE <> 'APPLICANT'
				AND REPLACE(DV.APPLICANTNAME, ' ', '') = REPLACE(RTRIM(LTRIM(IM.FNAME)) + ' ' + CASE 
						WHEN ISNULL(RTRIM(LTRIM(IM.MNAME)), '') = ''
							THEN ''
						ELSE RTRIM(LTRIM(IM.MNAME)) + ' '
						END + ISNULL(RTRIM(LTRIM(IM.LNAME)), ''), ' ', '')
			INNER JOIN IILHFC..TBL_TU_DOCUMENT_MASTER TD WITH (NOLOCK) ON TD.CATEGORY_ID = DV.CATEGORY_ID
				AND TD.ID = DV.DOC_ID
			LEFT JOIN IILHFC.DBO.TBL_CLIENTMASTER CM WITH (NOLOCK) ON CM.PROSPECTNO = DV.PROSPECTNO
			WHERE CM.ISTU = 0
				AND DV.IMAGETYPE IN (
					'LOS POA'
					,'LOS_POCA'
					,'LOS_POPA'
					) -- CHANGES DONE BY RAJU JADHAV
				AND (
					LOWER(TD.DOCUMENT_NAME) LIKE '%AADHAAR%'
					OR LOWER(TD.DOCUMENT_NAME) LIKE '%PASSPORT%'
					OR LOWER(TD.DOCUMENT_NAME) LIKE '%DRIVING%'
					OR LOWER(TD.DOCUMENT_NAME) LIKE '%VOTER ID%'
					)
				OR LOWER(TD.DOCUMENT_NAME) LIKE '%ekyc authentication%' -- ADDED BY RAJU JADHAV ON 28TH JULY 2022
			) AS T
		) AS TEST
	WHERE RN = 1

	--===============Applicant POI================================================================================================
	--SELECT  * INTO #CTEDOCUMENTS_POI 
	--FROM (
	--SELECT ROW_NUMBER() OVER (PARTITION BY IORG.CUID ORDER BY IORG.CUID, DV.SRNO DESC) AS RN
	--		,DV.IMAGENAME
	--		,DV.SRNO
	--		,DV.APPLICANTTYPE
	--		,DV.APPLICANTNAME
	--		,IORG.PANNO
	--		,DV.MKRID
	--		,DV.IMAGEPATH AS FILEPATH
	--		,'.' + RIGHT(IMAGEPATH, CHARINDEX('.', REVERSE(IMAGEPATH)) - 1) AS FILEEXTENSION
	--		,CASE 
	--			WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PAN%'
	--				THEN 'PANCARD'
	--			WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%AADHAAR%'
	--				THEN 'AADHARCARD'
	--            WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PASSPORT%'
	--				THEN 'PASSPORT'
	--	        WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%DRIVING%'
	--				THEN 'DRIVINGLICENCE'
	--            WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%VOTER ID%'
	--				THEN 'VOTERID'
	--            WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%UTILITY BILLS%'
	--				THEN 'UTILITYBILL2M'
	--			WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%RENT AGREEMENT%'
	--				THEN 'EMPLOYERHOUSELETTER'
	--			--WHEN LOWER(TD.DOCUMENT_NAME) LIKE '%PHOTO%'
	--			--	THEN 'APPLICANTPHOTO'
	--			ELSE 'OTHERSPOACKYCIND'
	--			END AS ATTACHMENTCODE
	--		,DV.PROSPECTNO 
	--		,IORG.CUID AS SOURCESYSYTEMCUSTOMERCODE
	--		,'' AS CLIENTIMAGE
	--		,'N' AS PHOTOUPLOADED
	--		,GETDATE() AS ADDEDON,  DV.IMAGETYPE
	--FROM IILHFC.DBO.TBL_ICMDOCUMENTVALIDATE DV WITH (NOLOCK)
	--INNER JOIN #CTEDOCUMENTS_POA DP ON DV.PROSPECTNO=DP.PROSPECTNO
	--INNER JOIN IILHFC..INDV_ORGMASTER IORG WITH(NOLOCK) ON IORG.PROSPECTNO = DV.PROSPECTNO AND IORG.APPLICANTTYPE = DV.APPLICANTTYPE AND IORG.NAME = DV.APPLICANTNAME
	--INNER JOIN IILHFC.DBO.INDIVIDUAL_MASTER  IM WITH (NOLOCK) ON IORG.PROSPECTNO = IM.IND_PROSPECTNO AND DV.APPLICANTTYPE <> 'APPLICANT' 
	--   AND REPLACE(DV.APPLICANTNAME, ' ', '') = REPLACE(RTRIM(LTRIM(IM.FNAME))+' '+CASE WHEN ISNULL(RTRIM(LTRIM(IM.MNAME)),'')=''THEN''ELSE RTRIM(LTRIM(IM.MNAME))+' 'END+ ISNULL(RTRIM(LTRIM(IM.LNAME)),''), ' ', '')
	--INNER JOIN IILHFC..TBL_TU_DOCUMENT_MASTER TD WITH (NOLOCK) ON TD.CATEGORY_ID = DV.CATEGORY_ID AND TD.ID = DV.DOC_ID
	--LEFT JOIN IILHFC.DBO.TBL_CLIENTMASTER CM WITH (NOLOCK) ON CM.PROSPECTNO = DV.PROSPECTNO
	--WHERE  CM.ISTU = 0  AND DV.IMAGETYPE='LOS POI'
	--) AS T
	--WHERE RN=1
	--================================================================#APPLICANT==============================================================		
	--Log file 
	INSERT INTO CKYC_RecordNotUploadReasonLog (
		PackageName
		,ProspectNo
		,Cuid
		,Reason
		,CreatedOn
		,BatchId
		,CreatedBy
		,Business
		)
	SELECT 'LOSCoApplicant' AS PackageName
		,a.ProspectNo
		,a.Cuid
		,'' AS Reason
		,GETDATE() AS CreatedOn
		,@BatchId BatchId
		,'SYS' AS CreatedBy
		,'HFC' AS Business
	FROM #DAILY_PROSPECTNO A
	INNER JOIN #CTEDOCUMENTS_POA B ON A.prospectNo = b.ProspectNo
		AND a.cuid = b.SOURCESYSYTEMCUSTOMERCODE
	
	UNION ALL
	
	SELECT 'LOSCoApplicant' AS PackageName
		,a.ProspectNo
		,a.Cuid
		,'POA OR POI image Missing' AS Reason
		,GETDATE() AS CreatedOn
		,@BatchId BatchId
		,'SYS' AS CreatedBy
		,'HFC' AS Business
	FROM #DAILY_PROSPECTNO A
	LEFT JOIN #CTEDOCUMENTS_POA B ON A.prospectNo = b.ProspectNo
		AND a.cuid = b.SOURCESYSYTEMCUSTOMERCODE
	WHERE b.ProspectNo IS NULL

	--=====================================
	SELECT PROSPECTNO = APP.PROSPECTNO
		,APPLICANTTYPE = APP.ApplicantType
		,APPLICANTNAME = APP.ApplicantName
		--,APP.CUID
		,POA.SourceSysytemCustomerCode AS CUID
		,APP.Prefix
		,APP.FirstName
		,APP.MiddleName
		,APP.LastName
		,APP.FatherPrefix
		,APP.[FatherFirstName]
		,APP.FatherMiddleName AS [FatherMiddleName]
		,APP.FatherLastName AS [FatherLastName]
		,APP.SpousePrefix
		,APP.SpouseFirstName
		,APP.SpouseMiddleName
		,APP.SpouseLastName
		,APP.MotherPrefix
		,APP.MotherFirstName
		,APP.MotherMiddleName
		,APP.MotherLastName
		,APP.Sex
		,MaritalStatus = CASE 
			WHEN APP.IsMarried = 'MARRIED'
				THEN 'M'
			WHEN APP.IsMarried = 'UnMarried'
				THEN 'U'
			ELSE 'O'
			END
		,[Citizenship] = 'IN'
		,OccupationType = CASE 
			WHEN APP.OccupationType IN (
					'Business'
					,'SENP'
					)
				THEN 'B-01'
			WHEN APP.OccupationType IN (
					'Others'
					,'OTH'
					,'Other'
					)
				THEN 'O-01'
			WHEN APP.OccupationType IN (
					'SAL'
					,'Salaried'
					)
				THEN 'S-02'
			WHEN APP.OccupationType = 'SEP'
				THEN 'O-02'
			WHEN APP.OccupationType IN (
					'HUF'
					,'HW'
					)
				THEN 'O-02'
			ELSE 'X-01'
			END
		,DateofBirth = APP.DOB
		,[ResidentialStatus] = '01'
		,EmailId = APP.EmailAddress
		,APP.CreatedTime
		,KYCPlaceOfDeclaration = APP.branch
		,KYCVerificationDate = APP.CreatedTime
		,KYCEmployeeName = COALESCE(emp.em_Name, emp1.EmployeeName)
		,KYCEmployeeDesignation = COALESCE(emp.em_Designation, '')
		,KYCVerificationBranch = 'HO'
		,KYCEmployeeCode = APP.MKRID
		,PermanentCKYCAddType = '01'
		,PermanentCountry = 'IN'
		--,PermanentPin = COALESCE(CA.AD_PinCode, CA1.AD_PinCode,Iorg1.Pincode, Iorg2.Pincode)
		,PermanentPin = CASE 
			WHEN rtrim(ltrim(isnull(CA.AD_PinCode, ''))) != ''
				THEN CA.AD_PinCode
			WHEN rtrim(ltrim(isnull(CA1.AD_PinCode, ''))) != ''
				THEN CA1.AD_PinCode
			WHEN rtrim(ltrim(isnull(Iorg1.Pincode, ''))) != ''
				THEN Iorg1.Pincode
			WHEN rtrim(ltrim(isnull(Iorg2.Pincode, ''))) != ''
				THEN Iorg2.Pincode
			ELSE ''
			END
		--,PermanentAddressLine1 = COALESCE(CA.ad_Add1, CA1.ad_Add1,iorg1.add1,  Iorg2.add1)
		,PermanentAddressLine1 = SUBSTRING(CASE 
				WHEN rtrim(ltrim(isnull(CA.ad_Add1, ''))) != ''
					THEN CA.ad_Add1
				WHEN rtrim(ltrim(isnull(CA1.ad_Add1, ''))) != ''
					THEN CA1.ad_Add1
				WHEN rtrim(ltrim(isnull(iorg1.add1, ''))) != ''
					THEN iorg1.add1
				WHEN rtrim(ltrim(isnull(iorg2.add1, ''))) != ''
					THEN iorg2.add1
				ELSE ''
				END, 1, 55) -- SUBSTRING ADDED BY RAJU JADHAV ON 22ND JULY 2022
		--,PermanentAddressLine2 = COALESCE(CA.ad_Add2, CA1.ad_Add2,iorg1.add2,  Iorg2.add2)
		,PermanentAddressLine2 = CASE 
			WHEN rtrim(ltrim(isnull(CA.ad_Add2, ''))) != ''
				THEN CA.ad_Add2
			WHEN rtrim(ltrim(isnull(CA1.ad_Add2, ''))) != ''
				THEN CA1.ad_Add2
			WHEN rtrim(ltrim(isnull(iorg1.add2, ''))) != ''
				THEN iorg1.add2
			WHEN rtrim(ltrim(isnull(iorg2.add2, ''))) != ''
				THEN iorg2.add2
			ELSE ''
			END
		--,PermanentAddressLine3 = COALESCE(CA.ad_Add3, CA1.ad_Add3,iorg1.add3,  Iorg2.add3)
		,PermanentAddressLine3 = CASE 
			WHEN rtrim(ltrim(isnull(CA.ad_Add3, ''))) != ''
				THEN CA.ad_Add3
			WHEN rtrim(ltrim(isnull(CA1.ad_Add3, ''))) != ''
				THEN CA1.ad_Add3
			WHEN rtrim(ltrim(isnull(iorg1.add3, ''))) != ''
				THEN iorg1.add3
			WHEN rtrim(ltrim(isnull(iorg2.add3, ''))) != ''
				THEN iorg2.add3
			ELSE ''
			END
		,PermanentDistrict = ''
		,PermanentCity = CASE 
			WHEN ISNULL(cm.City_Description, '') <> ''
				THEN cm.City_Description
			WHEN ISNULL(cm1.City_Description, '') <> ''
				THEN cm1.City_Description
			WHEN ISNULL(cm3.City_Description, '') <> ''
				THEN cm3.City_Description
			ELSE cm2.City_Description
			END
		,PermanentState = CASE 
			WHEN ISNULL(SM.State_Description, '') <> ''
				THEN SM.State_Description
			WHEN ISNULL(SM1.State_Description, '') <> ''
				THEN SM1.State_Description
			WHEN ISNULL(SM3.State_Description, '') <> ''
				THEN SM3.State_Description
			ELSE SM2.State_Description
			END
		,PermanentAddressProof = CASE 
			--WHEN POA.AttachmentCode IN (
			--		'Bank Statement'
			--		,'BANKSTATEMENT'
			--		)
			--	THEN 'BankStatement'
			--WHEN POA.AttachmentCode = 'Declaration_Of_Same_Address' THEN ''
			--WHEN POA.AttachmentCode = 'Govt_Authority_Letter' THEN 'OthersPOACKYCInd'
			--WHEN POA.AttachmentCode = 'Insurance_Policy'
			--	THEN 'OthersPOACKYCInd'
			--WHEN POA.AttachmentCode = 'Pension_Payment_Orders'
			--	THEN 'PensionOrder'
			WHEN POA.AttachmentCode = 'POA_AADHAR'
				THEN 'AadharCard'
					--WHEN POA.AttachmentCode = 'POA_Account_Statement'
					--	THEN 'BankStatement'
					--WHEN POA.AttachmentCode = 'POA_Credit_card_statement' THEN 'OthersPOACKYCInd'
			WHEN POA.AttachmentCode = 'POA_DRIVINGLICENSE'
				THEN 'DrivingLicence'
					--WHEN POA.AttachmentCode = 'POA_Letter_By_Employer' THEN 'EmployerHouseLetter'
			WHEN POA.AttachmentCode = 'POA_PASSPORT'
				THEN 'Passport'
					--WHEN POA.AttachmentCode = 'POA_Ration_Card'
					--	THEN 'RationCard'
					--WHEN POA.AttachmentCode = 'POA_RENTAGREEMENT'
					--	THEN 'EmployerHouseLetter'
					--WHEN POA.AttachmentCode = 'POA_Residence_Addres_Cert' THEN 'EmployerHouseLetter'
					--WHEN POA.AttachmentCode = 'POA_Sale_Deed' THEN 'OthersPOACKYCInd'
					--		--WHEN POA.AttachmentCode = 'POA_Tax_Receipt' THEN 'OthersPOACKYCInd'
					--WHEN POA.AttachmentCode = 'POA_UTILITYBILL'
					--	THEN 'Utilitybill2m'
			WHEN POA.AttachmentCode = 'POA_VOTERID'
				THEN 'VoterID'
					--WHEN POA.AttachmentCode = 'Post_Office_Bank_Account'
					--	THEN 'BankStatement'
					--WHEN POA.AttachmentCode = 'PreviousAddressProof'
					--	THEN ''
			ELSE POA.AttachmentCode
			END
		,CorrespondenceAddressProof = CASE 
			WHEN POA.AttachmentCode = 'POA_AADHAR'
				THEN 'AadharCard'
			WHEN POA.AttachmentCode = 'POA_DRIVINGLICENSE'
				THEN 'DrivingLicence'
			WHEN POA.AttachmentCode = 'POA_PASSPORT'
				THEN 'Passport'
			WHEN POA.AttachmentCode = 'POA_VOTERID'
				THEN 'VoterID'
			ELSE POA.AttachmentCode
			END
		,CorrespondenceGlobalCountry = 'IN'
		--,CorrespondenceGlobalPin = COALESCE(CA.AD_PinCode, CA1.AD_PinCode,Iorg1.Pincode,Iorg2.Pincode)
		,CorrespondenceGlobalPin = CASE 
			WHEN rtrim(ltrim(isnull(CA.AD_PinCode, ''))) != ''
				THEN CA.AD_PinCode
			WHEN rtrim(ltrim(isnull(CA1.AD_PinCode, ''))) != ''
				THEN CA1.AD_PinCode
			WHEN rtrim(ltrim(isnull(Iorg1.Pincode, ''))) != ''
				THEN Iorg1.Pincode
			WHEN rtrim(ltrim(isnull(Iorg2.Pincode, ''))) != ''
				THEN Iorg2.Pincode
			ELSE ''
			END
		--,CorrespondenceGlobalAddressLine1 = COALESCE(CA.ad_Add1, CA1.ad_Add1,iorg1.add1,  Iorg2.add1)
		,CorrespondenceGlobalAddressLine1 = CASE 
			WHEN rtrim(ltrim(isnull(CA.ad_Add1, ''))) != ''
				THEN CA.ad_Add1
			WHEN rtrim(ltrim(isnull(CA1.ad_Add1, ''))) != ''
				THEN CA1.ad_Add1
			WHEN rtrim(ltrim(isnull(iorg1.add1, ''))) != ''
				THEN iorg1.add1
			WHEN rtrim(ltrim(isnull(iorg2.add1, ''))) != ''
				THEN iorg2.add1
			ELSE ''
			END
		--,CorrespondenceGlobalAddressLine2 = COALESCE(CA.ad_Add2, CA1.ad_Add2,iorg1.add2,  Iorg2.add2)
		,CorrespondenceGlobalAddressLine2 = CASE 
			WHEN rtrim(ltrim(isnull(CA.ad_Add2, ''))) != ''
				THEN CA.ad_Add2
			WHEN rtrim(ltrim(isnull(CA1.ad_Add2, ''))) != ''
				THEN CA1.ad_Add2
			WHEN rtrim(ltrim(isnull(iorg1.add2, ''))) != ''
				THEN iorg1.add2
			WHEN rtrim(ltrim(isnull(iorg2.add2, ''))) != ''
				THEN iorg2.add2
			ELSE ''
			END
		--,CorrespondenceGlobalAddressLine3 = COALESCE(CA.ad_Add3, CA1.ad_Add3,iorg1.add3,  Iorg2.add3)
		,CorrespondenceGlobalAddressLine3 = CASE 
			WHEN rtrim(ltrim(isnull(CA.ad_Add3, ''))) != ''
				THEN CA.ad_Add3
			WHEN rtrim(ltrim(isnull(CA1.ad_Add3, ''))) != ''
				THEN CA1.ad_Add3
			WHEN rtrim(ltrim(isnull(iorg1.add3, ''))) != ''
				THEN iorg1.add3
			WHEN rtrim(ltrim(isnull(iorg2.add3, ''))) != ''
				THEN iorg2.add3
			ELSE ''
			END
		,CorrespondenceGlobalDistrict = ''
		--,CorrespondenceGlobalCity = COALESCE(cm.City_Description, cm1.City_Description,cm3.City_Description,cm2.City_Description)
		,CorrespondenceGlobalCity = CASE 
			WHEN rtrim(ltrim(ISNULL(cm.City_Description, ''))) != ''
				THEN cm.City_Description
			WHEN rtrim(ltrim(ISNULL(cm1.City_Description, ''))) != ''
				THEN cm1.City_Description
			WHEN rtrim(ltrim(ISNULL(cm3.City_Description, ''))) != ''
				THEN cm3.City_Description
			ELSE cm2.City_Description
			END
		,CorrespondenceGlobalState = CASE 
			WHEN ISNULL(SM.State_Description, '') <> ''
				THEN SM.State_Description
			WHEN ISNULL(SM1.State_Description, '') <> ''
				THEN SM1.State_Description
			WHEN ISNULL(SM3.State_Description, '') <> ''
				THEN SM3.State_Description
			ELSE SM2.State_Description
			END
		,JurisdictionOfResidence = 'IN'
		,CountryOfBirth = 'IN'
		,'' AS [BirthCity]
		,'' AS [TaxIdentificationNumber]
		,'' AS [TaxResidencyAddressLine1]
		,'' AS [TaxResidencyAddressLine2]
		,'' AS [TaxResidencyAddressLine3]
		,'' AS [TaxResidencyPin]
		,'' AS [TaxResidencyDistrict]
		--,TaxResidencyCity = COALESCE(cm.City_Description, cm1.City_Description,cm2.City_Description,cm3.City_Description)
		,TaxResidencyCity = CASE 
			WHEN rtrim(ltrim(ISNULL(cm.City_Description, ''))) != ''
				THEN cm.City_Description
			WHEN rtrim(ltrim(ISNULL(cm1.City_Description, ''))) != ''
				THEN cm1.City_Description
			WHEN rtrim(ltrim(ISNULL(cm3.City_Description, ''))) != ''
				THEN cm3.City_Description
			ELSE cm2.City_Description
			END
		,TaxResidencyState = ''
		,TaxResidencyCountry = 'IN'
		,'' AS [ResidentialSTDCode]
		,'' AS [ResidentialTelephoneNumber]
		,'' AS [OfficeSTDCode]
		,'' AS [OfficeTelephoneNumber]
		,'' AS [MobileISD]
		,MobileNumber = TelephoneNumber
		,'' AS [FaxSTD]
		,'' AS [FaxNumber]
		,'' AS [CKYCID]
		,PassportNumber = CASE 
			WHEN isnull(AIMG.Passport, '') != ''
				THEN isnull(AIMG.Passport, '')
			ELSE NULL
			END
		,PassportExpiryDate = CASE 
			WHEN isnull(AIMG.Passport, '') != ''
				THEN AIMG.PPValidity
			ELSE NULL
			END
		,VoterIdCard = ISNULL(AIMG.VotersID, '')
		,PAN = isnull(AIMG.PANNo, '')
		,DrivingLicenseNumber = CASE 
			WHEN isnull(AIMG.DrivingLicense, '') != ''
				THEN isnull(AIMG.DrivingLicense, '')
			ELSE NULL
			END
		,DrivingLicenseExpiryDate = CASE 
			WHEN isnull(AIMG.DrivingLicense, '') != ''
				THEN isnull(AIMG.DLExpiry, '')
			ELSE NULL
			END
		,Aadhaar = CASE 
			WHEN isnull(AIMG.Adharno, '') != ''
				THEN isnull(AIMG.Adharno, '')
			ELSE NULL
			END
		,'' AS [NREGA]
		,CKYCPOIOtherCentralGovtID = ''
		,'' AS [CKYCPOIS01IDNumber]
		,'' AS [CKYCPOIS02IDNumber]
		--,ProofOfIDSubmitted=POI.FIELD_VALUE, 
		,ProofOfIDSubmitted = ''
		,'' AS [CustomerDemiseDate]
		,0 AS [Minor]
		,'' AS [SourcesystemRelatedPartyode]
		,'' AS [RelatedPersonType]
		,'' AS [RelatedPersonPrefix]
		,'' AS [RelatedPersonFirstName]
		,'' AS [RelatedPersonMiddleName]
		,'' AS [RelatedPersonLastName]
		,'' AS [RelatedPersonCKYCID]
		,'' AS [RelatedPersonPassportNumber]
		,'' AS [RelatedPersonPassportExpiryDate]
		,'' AS [RelatedPersonVoterIdCard]
		,'' AS [RelatedPersonPAN]
		,'' AS [RelatedPersonDrivingLicenseNumber]
		,'' AS [RelatedPersonDrivingLicenseExpiryDate]
		,'' AS [RelatedPersonAadhaar]
		,'' AS [RelatedPersonNREGA]
		,'' AS [RelatedPersonCKYCPOIOtherCentralGovtID]
		,'' AS [RelatedPersonCKYCPOIS01IDNumber]
		,'' AS [RelatedPersonCKYCPOIS02IDNumber]
		,'' AS [RelatedPersonProofOfIDSubmitted]
		,'' AS [SourceSystemSegment]
		,'' AS [AppRefNumberforImages]
		,'' AS [HolderforImages]
		,BranchCode = APP.BRANCH
		,APP.ProspectStatus AS CustomerStatus
		,FORMAT(CONVERT(DATETIME, APP.CreatedTime, 6), 'dd-MMM-yyyy') AS CustomerStatusEffectiveDate
		,ROW_NUMBER() OVER (
			PARTITION BY [POA].[SourceSysytemCustomerCode] ORDER BY [APP].[ProspectCreationDate] DESC
			) AS [rn]
	INTO #APPLICANT
	FROM IILHFC.dbo.vw_AllApplicantsDetails APP WITH (NOLOCK)
	INNER JOIN IILHFC..Indv_OrgMaster IORG WITH (NOLOCK) ON IORG.ProspectNo = APP.Prospectno
		AND IORG.ApplicantType = APP.ApplicantType
		AND IORG.name = APP.ApplicantName
	INNER JOIN #APPLICANTIMAGE AIMG ON AIMG.Prospectno = APP.Prospectno
		AND IORG.CUID = AIMG.SourceSysytemCustomerCode
		AND AIMG.ApplicantType = APP.ApplicantType
		AND AIMG.ApplicantName = APP.ApplicantName
	INNER JOIN #CTEDOCUMENTS_POA POA WITH (NOLOCK) ON POA.PROSPECTNO = APP.PROSPECTNO
		AND IORG.CUID = POA.SourceSysytemCustomerCode
		AND POA.ApplicantType = APP.ApplicantType
		AND POA.ApplicantName = APP.ApplicantName
	--INNER JOIN #CTEDOCUMENTS_POI POI WITH (NOLOCK) ON POI.PROSPECTNO = APP.PROSPECTNO AND IORG.CUID =POI.SourceSysytemCustomerCode 
	--	AND POI.ApplicantType = APP.ApplicantType
	--	AND POI.ApplicantName = APP.ApplicantName
	LEFT JOIN IILHFC.dbo.tbl_clientaddresses CA WITH (NOLOCK) ON CA.AD_PROSPECTNO = APP.PROSPECTNO
		AND APP.ApplicantType = CA.ApplicantType
		AND CA.ApplicantName = APP.ApplicantName
		AND ca.ad_AddressType = 'PERMANENT RESIDENCE'
	LEFT JOIN IILHFC.dbo.tbl_clientaddresses CA1 WITH (NOLOCK) ON CA1.AD_PROSPECTNO = APP.PROSPECTNO
		AND APP.ApplicantType = CA1.ApplicantType
		AND CA1.ApplicantName = APP.ApplicantName
		AND CA1.ad_AddressType = 'CURRENT RESIDENCE'
	LEFT JOIN IILHFC.dbo.City_Master cm WITH (NOLOCK) ON cm.City_Code = CA.ad_City
	LEFT JOIN IILHFC.dbo.state_master SM WITH (NOLOCK) ON SM.State_Code = CA.ad_State
	LEFT JOIN IILHFC.dbo.City_Master cm1 WITH (NOLOCK) ON cm1.City_Code = CA1.ad_City
	LEFT JOIN IILHFC.dbo.state_master SM1 WITH (NOLOCK) ON SM1.State_Code = CA1.ad_State
	--added on 22 Jan 2020 for the
	LEFT JOIN IILHFC.dbo.indv_orgmaster iorg2 WITH (NOLOCK) ON APP.PROSPECTNO = iorg2.ProspectNo
		AND APP.ApplicantType = iorg2.ApplicantType
		AND iorg2.name = APP.ApplicantName
		AND iorg2.AddressType = 'CURRENT RESIDENCE'
	LEFT JOIN IILHFC.dbo.state_master SM2 WITH (NOLOCK) ON iorg2.StateCode = SM2.State_Code
		AND SM2.State_status = 'Y'
	LEFT JOIN IILHFC.dbo.City_Master CM2 WITH (NOLOCK) ON iorg2.CityCode = CM2.City_Code
		AND (
			(
				SM2.State_Code = 'XX'
				AND 1 = 1
				)
			OR (CM2.CState_code = SM2.State_Code)
			)
		AND CM2.City_status = 'Y'
	LEFT JOIN IILHFC.dbo.indv_orgmaster iorg1 WITH (NOLOCK) ON APP.PROSPECTNO = iorg1.ProspectNo
		AND APP.ApplicantType = iorg1.ApplicantType
		AND iorg1.name = APP.ApplicantName
		AND iorg1.AddressType = 'PERMANENT RESIDENCE'
	LEFT JOIN IILHFC.dbo.state_master SM3 WITH (NOLOCK) ON iorg1.StateCode = SM3.State_Code
		AND SM3.State_status = 'Y'
	LEFT JOIN IILHFC.dbo.City_Master CM3 WITH (NOLOCK) ON iorg1.CityCode = CM3.City_Code
		AND (
			(
				SM3.State_Code = 'XX'
				AND 1 = 1
				)
			OR (CM3.CState_code = SM3.State_Code)
			)
		AND CM3.City_status = 'Y'
	LEFT JOIN masterhub.dbo.EmpMasterAll EMP WITH (NOLOCK) ON EMP.em_EmpID = APP.Mkrid
	LEFT JOIN IILHFC.dbo.OutSourceEmployee EMP1 WITH (NOLOCK) ON EMP1.EmpId = APP.Mkrid

	--LEFT OUTER JOIN [UCID].[dbo].[Unique_Customers] AS [U] ON [APP].[CUID] = [U].[CustomerID]
	--	AND [U].[Business] = 'HFC'
	--WHERE
	--	APP.ProspectStatus = 'ACT'
	INSERT INTO Clients.dbo.StagingCustom217CustomerFlat (
		ParentCompany
		,TransactionID
		,SourceSystemName
		,SourceSystemCustomerCode
		,SourceSystemCustomerCreationDate
		,IsSmallCustomer
		,EkycOTPbased
		,RecordIdentifier
		,Segments
		,SegmentStartDate
		,ProductSegments
		,CustomerStatus
		,CustomerStatusEffectiveDate
		,RelatedPartyStatus
		,RelatedPartyStatusEffectiveDate
		,CustomerType
		,CustomerSubType
		,Prefix
		,FirstName
		,MiddleName
		,LastName
		,MaidenPrefix
		,MaidenFirstName
		,MaidenMiddleName
		,MaidenLastName
		,FatherPrefix
		,FatherFirstName
		,FatherMiddleName
		,FatherLastName
		,SpousePrefix
		,SpouseFirstName
		,SpouseMiddleName
		,SpouseLastName
		,MotherPrefix
		,MotherFirstName
		,MotherMiddleName
		,MotherLastName
		,Gender
		,MaritalStatus
		,Citizenship
		,CountryOfResidence
		,OccupationType
		,ActivitySector
		,NatureOfBusiness
		,NatureOfBusinessOther
		,DateofBirth
		,WorkEmail
		,PersonalEmail
		,KYCDateOfDeclaration
		,KYCPlaceOfDeclaration
		,KYCVerificationDate
		,KYCEmployeeName
		,KYCEmployeeDesignation
		,KYCVerificationBranch
		,KYCEmployeeCode
		,PermanentCKYCAddressType
		,PlotnoSurveynoHouseFlatno
		,PermanentAddressCountry
		,PermanentAddressPinCode
		,PermanentAddressLine1
		,PermanentAddressLine2
		,PermanentAddressLine3
		,PermanentAddressDistrict
		,PermanentAddressCity
		,PermanentAddressState
		,PermanentAddressProof
		,CorrespondenceAddressCountry
		,CorrespondenceAddressPinCode
		,CorrespondenceAddressLine1
		,CorrespondenceAddressLine2
		,CorrespondenceAddressLine3
		,CorrespondenceAddressDistrict
		,CorrespondenceAddressCity
		,CorrespondenceAddressState
		,CorrespondenceAddressProof
		,WorkAddressCountry
		,WorkAddressPinCode
		,WorkAddressLine1
		,WorkAddressLine2
		,WorkAddressLine3
		,WorkAddressDistrict
		,WorkAddressCity
		,WorkAddressState
		,CountryOfBirth
		,BirthCity
		,TaxResidencyCountry
		,TaxIdentificationNumber
		,TaxResidencyAddressCountry
		,TaxResidencyAddressLine1
		,TaxResidencyAddressLine2
		,TaxResidencyAddressLine3
		,TaxResidencyAddressPinCode
		,TaxResidencyAddressDistrict
		,TaxResidencyAddressCity
		,TaxResidencyAddressState
		,DeskPersonalISDCode
		,DeskPersonalSTDCode
		,DeskPersonalTelephoneNumber
		,DeskWorkISDCode
		,DeskWorkSTDCode
		,DeskWorkTelephoneNumber
		,WorkMobileISD
		,WorkMobileNumber
		,PersonalMobileISD
		,PersonalMobileNumber
		,CKYCID
		,PassportIssueCountry
		,PassportNumber
		,PassportExpiryDate
		,VoterIdCard
		,PAN
		,DrivingLicenseNumber
		,DrivingLicenseExpiryDate
		,Aadhaar
		,AadhaarVaultReferenceNumber
		,AadhaarToken
		,AadhaarVirtualId
		,NREGA
		,CKYCPOIOtherCentralGovtID
		,CKYCPOIS01IDNumber
		,CKYCPOIS02IDNumber
		,NationalID
		,TaxIdLocal
		,CompanyRegistrationNumber
		,CompanyRegistrationCountry
		,GIIN
		,OthersInd
		,OthersNonInd
		,ProofOfIDSubmitted
		,Minor
		,ApplicationRefNumber
		,HolderforImages
		,IntermediaryCode
		,Listed
		,Industry
		,Nationality
		,CountryofOperation
		,RegulatoryAMLRisk
		,RegAMLSpecialCategory
		,RegAMLSpecialCategoryStartDate
		,RegAMLSpecialCategoryEndDate
		,LastRiskReviewDate
		,NextRiskReviewDate
		,IncomeRange
		,ExactIncome
		,IncomeCurrency
		,IncomeEffectiveDate
		,IncomeDescription
		,IncomeProof
		,ExactNetworth
		,NetworthCurrency
		,NetworthEffectiveDate
		,NetworthDescription
		,NetworthProof
		,PEP
		,PEPClassification
		,AdverseMedia
		,AdverseMediaClassification
		,AdverseMediaDetails
		,InsiderInformation
		,Tags
		,FamilyCode
		,Channel
		,Links
		,ReputationClassification
		,IUPartyType
		,PropertyOwnerFlag
		,ContactPersonFirstName
		,ContactPersonMiddleName
		,ContactPersonLastName
		,ContactPersonDesignation
		,ContactPersonMobileISD
		,ContactPersonMobileNo
		,ContactPersonMobileISD2
		,ContactPersonMobileNo2
		,ContactPersonEmailId1
		,ContactPersonEmailId2
		,RMUserCode
		,RMType
		,RMFromDate
		,EducationalQualification
		,DesignationOthers
		,EmployerName
		,EmployerAddress
		,EmployerListed
		,EmployerOrganisationType
		,CurrentEmploymentInYears
		,ModuleApplicable
		,AddedBy
		,AddedOn
		,GUID
		,FormSixty
		,NPRLetter
		,KYCAttestationType
		,batchId
		,IsValid
		)
	SELECT 'HFC1' AS ParentCompany
		,TransactionID
		,[SourceSystemName]
		,SourcesystemCustCode AS [SourceSystemCustomerCode]
		,[SourceSystemCustomerCreationDate]
		,0 AS [IsSmallCustomer]
		,0 AS [EkycOTPbased]
		,'' AS RecordIdentifier
		,'' AS Segments
		,'' AS SegmentStartDate
		,'' AS ProductSegments
		,CASE 
			WHEN CustomerStatus = 'ACT'
				THEN 'Active'
			WHEN CustomerStatus = 'CLS'
				THEN 'Closed'
			ELSE 'InActive'
			END AS CustomerStatus
		,CustomerStatusEffectiveDate
		,'' RelatedPartyStatus
		,'' RelatedPartyStatusEffectiveDate
		,[ConstitutionType] AS CustomerType
		,'' CustomerSubType
		,[Prefix]
		,[FirstName]
		,[MiddleName]
		,[LastName]
		,[MaidenPrefix]
		,[MaidenFirstName]
		,[MaidenMiddleName]
		,[MaidenLastName]
		,[FatherPrefix]
		,[FatherFirstName]
		,[FatherMiddleName]
		,[FatherLastName]
		,[SpousePrefix]
		,[SpouseFirstName]
		,[SpouseMiddleName]
		,[SpouseLastName]
		,[MotherPrefix]
		,[MotherFirstName]
		,[MotherMiddleName]
		,[MotherLastName]
		,[Gender]
		,[MaritalStatus]
		,[Citizenship]
		,'' AS CountryOfResidence
		,[OccupationType]
		,'' AS ActivitySector
		,'' AS NatureOfBusiness
		,'' AS NatureOfBusinessOther
		,[DateofBirth]
		,'' AS WorkEmail
		,[EmailId] AS PersonalEmail
		,[KYCDateOfDeclaration]
		,[KYCPlaceOfDeclaration]
		,[KYCVerificationDate]
		,[KYCEmployeeName]
		,[KYCEmployeeDesignation]
		,[KYCVerificationBranch]
		,[KYCEmployeeCode]
		,Final.PermanentCKYCAddType AS [PermanentCKYCAddressType]
		,'' AS PlotnoSurveynoHouseFlatno
		,[PermanentCountry] AS PermanentAddressCountry
		,[PermanentPin] AS PermanentAddressPinCode
		,CASE 
			WHEN ISNULL([PermanentAddressLine1], '') = ''
				AND ISNULL([PermanentAddressLine2], '') != ''
				THEN ISNULL([PermanentAddressLine2], '')
			WHEN ISNULL([PermanentAddressLine1], '') = ''
				AND ISNULL([PermanentAddressLine2], '') = ''
				AND ISNULL([PermanentAddressLine3], '') != ''
				THEN ISNULL([PermanentAddressLine3], '')
			ELSE ISNULL([PermanentAddressLine1], '')
			END AS [PermanentAddressLine1]
		,CASE 
			WHEN ISNULL([PermanentAddressLine1], '') = ''
				AND ISNULL([PermanentAddressLine2], '') != ''
				THEN ''
			ELSE ISNULL([PermanentAddressLine2], '')
			END AS [PermanentAddressLine2]
		,CASE 
			WHEN ISNULL([PermanentAddressLine1], '') = ''
				AND ISNULL([PermanentAddressLine2], '') = ''
				AND ISNULL([PermanentAddressLine3], '') != ''
				THEN ''
			ELSE ISNULL([PermanentAddressLine3], '')
			END AS [PermanentAddressLine3]
		,[PermanentDistrict] AS PermanentAddressDistrict
		,dbo.RemoveRepeatingChars(dbo.RemoveSpecialCharacters([PermanentCity])) AS [PermanentAddressCity]
		,[PermanentState] AS PermanentAddressState
		,[PermanentAddressProof]
		,[CorrespondenceGlobalCountry] AS CorrespondenceAddressCountry
		,[CorrespondenceGlobalPin] AS CorrespondenceAddressPinCode
		,Final.CorrespondenceGlobalAddressLine1 AS [CorrespondenceAddressLine1]
		,Final.CorrespondenceGlobalAddressLine2 AS [CorrespondenceAddressLine2]
		,Final.CorrespondenceGlobalAddressLine3 AS [CorrespondenceAddressLine3]
		,[CorrespondenceGlobalDistrict] AS CorrespondenceAddressDistrict
		,[CorrespondenceGlobalCity] AS CorrespondenceAddressCity
		,[CorrespondenceGlobalState] AS CorrespondenceAddressState
		,CorrespondenceAddressProof AS CorrespondenceAddressProof
		,'' AS WorkAddressCountry
		,'' AS WorkAddressPinCode
		,'' AS WorkAddressLine1
		,'' AS WorkAddressLine2
		,'' AS WorkAddressLine3
		,'' AS WorkAddressDistrict
		,'' AS WorkAddressCity
		,'' AS WorkAddressState
		,[CountryOfBirth]
		,[BirthCity]
		,[JurisdictionOfResidence] AS TaxResidencyCountry
		,[TaxIdentificationNumber]
		,[TaxResidencyCountry] AS TaxResidencyAddressCountry
		,[TaxResidencyAddressLine1] AS TaxResidencyAddressLine1
		,[TaxResidencyAddressLine2] AS TaxResidencyAddressLine2
		,[TaxResidencyAddressLine3] AS TaxResidencyAddressLine3
		,[TaxResidencyPin] AS TaxResidencyAddressPinCode
		,[TaxResidencyDistrict] AS TaxResidencyAddressDistrict
		,dbo.RemoveRepeatingChars(dbo.RemoveSpecialCharacters(ISNULL([TaxResidencyCity], ''))) AS [TaxResidencyAddressCity]
		,[TaxResidencyState] AS TaxResidencyAddressState
		,'' AS DeskPersonalISDCode
		,[ResidentialSTDCode] AS DeskWorkSTDCode
		,[ResidentialTelephoneNumber] AS DeskPersonalTelephoneNumber
		,'' AS DeskWorkISDCode
		,[OfficeSTDCode] AS DeskWorkSTDCode
		,[OfficeTelephoneNumber] AS DeskWorkTelephoneNumber
		,'' AS [WorkMobileISD]
		,'' AS [WorkMobileNumber]
		,[MobileISD] AS PersonalMobileISD
		,[MobileNumber] AS PersonalMobileNumber
		,Final.CKYCID AS [CKYCNumber]
		,'IN' AS PassportIssueCountry
		,[PassportNumber] AS Passport
		,[PassportExpiryDate]
		,VoterIdCard AS VoterIdCard
		,[PAN] AS PAN
		,[DrivingLicenseNumber] AS DrivingLicenseNumber
		,[DrivingLicenseExpiryDate] AS DrivingLicenseExpiryDate
		,[Aadhaar] AS [Aadhaar]
		,'' AadhaarVaultReferenceNumber
		,'' AadhaarToken
		,'' AadhaarVirtualId
		,[NREGA]
		,[CKYCPOIOtherCentralGovtID]
		,[CKYCPOIS01IDNumber]
		,[CKYCPOIS02IDNumber]
		,'' AS NationalID
		,'' AS TaxIdLocal
		,'' AS CompanyRegistrationNumber
		,'' AS CompanyRegistrationCountry
		,'' AS GIIN
		,'' AS OthersInd
		,'' AS OthersNonInd
		,[ProofOfIDSubmitted]
		,Final.Minor AS [Minor]
		,Final.AppRefNumberforImages AS ApplicationRefNumber
		,[HolderforImages]
		,[BranchCode] AS IntermediaryCode
		,NULL AS Listed
		,NULL AS Industry
		,NULL AS Nationality
		,NULL AS CountryofOperation
		,NULL AS RegulatoryAMLRisk
		,NULL AS RegAMLSpecialCategory
		,NULL AS RegAMLSpecialCategoryStartDate
		,NULL AS RegAMLSpecialCategoryEndDate
		,NULL AS LastRiskReviewDate
		,NULL AS NextRiskReviewDate
		,NULL AS IncomeRange
		,NULL AS ExactIncome
		,NULL AS IncomeCurrency
		,NULL AS IncomeEffectiveDate
		,NULL AS IncomeDescription
		,NULL AS IncomeProof
		,NULL AS ExactNetworth
		,NULL AS NetworthCurrency
		,NULL AS NetworthEffectiveDate
		,NULL AS NetworthDescription
		,NULL AS NetworthProof
		,NULL AS PEP
		,NULL AS PEPClassification
		,NULL AS AdverseMedia
		,NULL AS AdverseMediaClassification
		,NULL AS AdverseMediaDetails
		,NULL AS InsiderInformation
		,NULL AS Tags
		,NULL AS FamilyCode
		,NULL AS Channel
		,NULL AS Links
		,NULL AS ReputationClassification
		,NULL AS IUPartyType
		,NULL AS PropertyOwnerFlag
		,NULL AS ContactPersonFirstName
		,NULL AS ContactPersonMiddleName
		,NULL AS ContactPersonLastName
		,NULL AS ContactPersonDesignation
		,NULL AS ContactPersonMobileISD
		,NULL AS ContactPersonMobileNo
		,NULL AS ContactPersonMobileISD2
		,NULL AS ContactPersonMobileNo2
		,NULL AS ContactPersonEmailId1
		,NULL AS ContactPersonEmailId2
		,NULL AS RMUserCode
		,NULL AS RMType
		,NULL AS RMFromDate
		,NULL AS EducationalQualification
		,NULL AS DesignationOthers
		,NULL AS EmployerName
		,NULL AS EmployerAddress
		,NULL AS EmployerListed
		,NULL AS EmployerOrganisationType
		,NULL AS CurrentEmploymentInYears
		,'CKYC' AS ModuleApplicable
		,KYCEmployeeCode AS AddedBy
		,GETDATE() AS AddedOn
		,'' AS GUID
		,CASE 
			WHEN ISNULL(PAN, '') != ''
				THEN 0
			ELSE 1
			END AS FORMSixty
		,'' AS NPRLetter
		,'03' AS KYCAttestationType
		,@BatchId AS batchId
		,'Y' AS IsValid
	FROM (
		SELECT 'HFC' + CONVERT(VARCHAR(8), GETDATE(), 112) + convert(VARCHAR(2), Datepart(HOUR, getdate())) + convert(VARCHAR(2), Datepart(MINUTE, getdate())) + RIGHT('0000000' + CONVERT(VARCHAR(10), DENSE_RANK() OVER (
						ORDER BY [CTE].[CUID]
						)), 10) AS [TransactionID]
			,'HFC' AS [SourceSystemName]
			,[CTE].[CUID] AS [SourcesystemCustCode]
			,0 AS [SmallCustomer]
			,0 AS [EkycOTPbased]
			,CASE 
				WHEN ISDATE([CTE].[CreatedTime]) = 1
					THEN FORMAT(CONVERT(DATETIME, [CTE].[CreatedTime], 6), 'dd-MMM-yyyy')
				ELSE NULL
				END AS [SourceSystemCustomerCreationDate]
			,'1' AS [ConstitutionType]
			,[CTE].[Prefix]
			,[CTE].[FirstName]
			,[CTE].[MiddleName]
			,[CTE].[LastName]
			,'' [MaidenPrefix]
			,--  CASE WHEN ISNULL([CTE].[MaidenName], '') <> '' THEN 'Mrs' ELSE '' END AS [MaidenPrefix], 
			'' [MaidenFirstName]
			,--[CTE].[MaidenName] AS [MaidenFirstName], 
			'' [MaidenMiddleName]
			,---[CTE].[MaidenMiddleName] AS [MaidenMiddleName], 
			'' [MaidenLastName]
			,-- [CTE].[MaidenLastName] AS [MaidenLastName], 
			CASE 
				WHEN ISNULL([CTE].[FatherFirstName], '') <> ''
					THEN 'Mr'
				ELSE ''
				END AS [FatherPrefix]
			,REPLACE([CTE].[FatherFirstName], '.', ' ') AS [FatherFirstName]
			,[CTE].[FatherMiddleName] AS [FatherMiddleName]
			,[CTE].[FatherLastName] AS [FatherLastName]
			---Spouse
			,CASE 
				WHEN ISNULL([CTE].[SpouseFirstName], '') <> ''
					AND ISNULL([CTE].[FatherFirstName], '') = ''
					THEN (
							CASE 
								WHEN [CTE].[Sex] = 'M'
									THEN 'Mrs'
								ELSE 'Mr'
								END
							)
				WHEN ISNULL([CTE].[SpouseFirstName], '') <> ''
					AND ISNULL([CTE].[FatherFirstName], '') = ''
					THEN 'Mrs'
				ELSE ''
				END AS [SpousePrefix]
			,CASE 
				WHEN ISNULL([CTE].[FatherFirstName], '') = ''
					THEN REPLACE(ISNULL([CTE].SpouseFirstName, ''), '.', ' ')
				END AS [SpouseFirstName]
			,[CTE].[SpouseMiddleName] AS [SpouseMiddleName]
			,[CTE].[SpouseLastName] AS [SpouseLastName]
			,CASE 
				WHEN ISNULL([CTE].[MotherFirstName], '') <> ''
					THEN 'Mrs'
				ELSE ''
				END AS [MotherPrefix]
			,REPLACE([CTE].[MotherFirstName], '.', '') AS [MotherFirstName]
			,[CTE].[MotherMiddleName] AS [MotherMiddleName]
			,[CTE].[MotherLastName] AS [MotherLastName]
			,[CTE].[Sex] AS [Gender]
			,[CTE].[MaritalStatus]
			,'IN' AS [Citizenship]
			,[CTE].[OccupationType]
			,CASE 
				WHEN ISDATE([CTE].[DateofBirth]) = 1
					THEN FORMAT(CONVERT(DATETIME, [CTE].[DateofBirth]), 'dd-MMM-yyyy')
				ELSE NULL
				END AS [DateofBirth]
			,'01' AS [ResidentialStatus]
			,'' [EmailId]
			--[CTE].[Email] AS [EmailId], 
			,CASE 
				WHEN ISDATE([CTE].[CreatedTime]) = 1
					THEN FORMAT(CONVERT(DATETIME, [CTE].[CreatedTime], 6), 'dd-MMM-yyyy')
				ELSE NULL
				END AS [KYCDateOfDeclaration]
			,[CTE].[KYCPlaceOfDeclaration] AS [KYCPlaceOfDeclaration]
			,CASE 
				WHEN ISDATE([CTE].[CreatedTime]) = 1
					THEN FORMAT(CONVERT(DATETIME, [CTE].[CreatedTime], 6), 'dd-MMM-yyyy')
				ELSE NULL
				END AS [KYCVerificationDate]
			,ISNULL([CTE].[KYCEmployeeName], '') AS [KYCEmployeeName]
			,ISNULL([CTE].[KYCEmployeeDesignation], '') AS [KYCEmployeeDesignation]
			,'HO' AS [KYCVerificationBranch]
			,[CTE].[KYCEmployeeCode] AS [KYCEmployeeCode]
			,'01' AS [PermanentCKYCAddType]
			,'IN' AS [PermanentCountry]
			,[CTE].[PermanentPin] AS [PermanentPin]
			,[CTE].[PermanentAddressLine1] AS [PermanentAddressLine1]
			,--- replace special character with space
			[CTE].[PermanentAddressLine2] AS [PermanentAddressLine2]
			,--- replace special character with space
			[CTE].[PermanentAddressLine3] AS [PermanentAddressLine3]
			,--- replace special character with space
			'' AS [PermanentDistrict]
			,ISNULL([CTE].[PermanentCity], '') AS [PermanentCity]
			,'' AS [PermanentState]
			,[CTE].[PermanentAddressProof]
			,'IN' AS [CorrespondenceGlobalCountry]
			,[CTE].[CorrespondenceGlobalPin] AS [CorrespondenceGlobalPin]
			,[CTE].[CorrespondenceGlobalAddressLine1] AS [CorrespondenceGlobalAddressLine1]
			,--- replace special character with space
			[CTE].[CorrespondenceGlobalAddressLine2] AS [CorrespondenceGlobalAddressLine2]
			,--- replace special character with space
			[CTE].[CorrespondenceGlobalAddressLine3] AS [CorrespondenceGlobalAddressLine3]
			,--- replace special character with space
			'' AS [CorrespondenceGlobalDistrict]
			,ISNULL([CTE].[CorrespondenceGlobalCity], '') AS [CorrespondenceGlobalCity]
			,'' AS [CorrespondenceGlobalState]
			,'IN' AS [JurisdictionOfResidence]
			,'IN' AS [CountryOfBirth]
			,'' AS [BirthCity]
			,'' AS [TaxIdentificationNumber]
			,'' AS [TaxResidencyAddressLine1]
			,'' AS [TaxResidencyAddressLine2]
			,'' AS [TaxResidencyAddressLine3]
			,'' AS [TaxResidencyPin]
			,'' AS [TaxResidencyDistrict]
			,'' AS [TaxResidencyCity]
			,'' AS [TaxResidencyState]
			,'IN' AS [TaxResidencyCountry]
			,NULL AS [ResidentialSTDCode]
			,NULL AS [ResidentialTelephoneNumber]
			,NULL AS [OfficeSTDCode]
			,NULL AS [OfficeTelephoneNumber]
			,NULL AS [MobileISD]
			,CASE 
				WHEN ISNUMERIC(LEFT([CTE].[MobileNumber], 10)) = 1
					THEN LEFT([CTE].[MobileNumber], 10)
				ELSE NULL
				END AS [MobileNumber]
			,NULL AS [FaxSTD]
			,NULL AS [FaxNumber]
			,NULL AS [CKYCID]
			,CASE 
				WHEN [CTE].[PermanentAddressProof] = 'Passport'
					THEN [CTE].[PassportNumber]
				ELSE ''
				END AS [PassportNumber]
			,CASE 
				WHEN [CTE].[PermanentAddressProof] = 'Passport'
					AND ISDATE([CTE].[PassportExpiryDate]) = 1
					THEN [CTE].[PassportExpiryDate]
				ELSE NULL
				END AS [PassportExpiryDate]
			,CASE 
				WHEN isnull([CTE].[VoterIdCard], '') != ''
					THEN isnull([CTE].[VoterIdCard], '')
				ELSE ''
				END AS [VoterIdCard]
			,CASE 
				WHEN [CTE].[ProofOfIDSubmitted] = 'pancard'
					OR (
						ISNULL([CTE].[FatherFirstName], '') = ''
						AND REPLACE([CTE].[SpouseFirstName], '.', ' ') <> ''
						)
					THEN [CTE].[PAN]
				ELSE ''
				END AS [PAN]
			,CASE 
				WHEN [CTE].[PermanentAddressProof] = 'DrivingLicence'
					THEN isnull([CTE].[DrivingLicenseNumber], '')
				ELSE ''
				END AS [DrivingLicenseNumber]
			,CASE 
				WHEN [CTE].[PermanentAddressProof] = 'DrivingLicence'
					AND ISDATE([CTE].[DrivingLicenseExpiryDate]) = 1
					THEN [CTE].[DrivingLicenseExpiryDate]
				ELSE NULL
				END AS [DrivingLicenseExpiryDate]
			,CASE 
				WHEN ISNUMERIC(LEFT([CTE].[Aadhaar], 12)) = 1
					THEN [CTE].[Aadhaar]
				ELSE NULL
				END AS [Aadhaar]
			,'' AS [NREGA]
			,'' AS [CKYCPOIOtherCentralGovtID]
			,'' AS [CKYCPOIS01IDNumber]
			,'' AS [CKYCPOIS02IDNumber]
			,[CTE].[ProofOfIDSubmitted] AS [ProofOfIDSubmitted]
			,NULL AS [CustomerDemiseDate]
			,'0' AS [Minor]
			,'' AS [SourcesystemRelatedPartyCode]
			,'' AS [RelatedPersonType]
			,'' AS [RelatedPersonPrefix]
			,'' AS [RelatedPersonFirstName]
			,'' AS [RelatedPersonMiddleName]
			,'' AS [RelatedPersonLastName]
			,NULL AS [RelatedPersonCKYCID]
			,'' AS [RelatedPersonPassportNumber]
			,NULL AS [RelatedPersonPassportExpiryDate]
			,'' AS [RelatedPersonVoterIdCard]
			,'' AS [RelatedPersonPAN]
			,'' AS [RelatedPersonDrivingLicenseNumber]
			,NULL AS [RelatedPersonDrivingLicenseExpiryDate]
			,NULL AS [RelatedPersonAadhaar]
			,'' AS [RelatedPersonNREGA]
			,'' AS [RelatedPersonCKYCPOIOtherCentralGovtID]
			,'' AS [RelatedPersonCKYCPOIS01IDNumber]
			,'' AS [RelatedPersonCKYCPOIS02IDNumber]
			,'' AS [RelatedPersonProofOfIDSubmitted]
			,'' AS [SourceSystemSegment]
			,[CTE].ProspectNo AS [AppRefNumberforImages]
			,'' AS [HolderforImages]
			,[CTE].BranchCode BranchCode
			--,[CTE].ProspectNo AS ProspectNo
			,CASE 
				WHEN ISNULL([CTE].FatherFirstName, '') <> ''
					OR ISNULL([CTE].SpouseFirstName, '') <> ''
					THEN 'Y'
				ELSE 'N'
				END AS [IsValid]
			,CustomerStatus
			,CustomerStatusEffectiveDate
			,CorrespondenceAddressProof
		FROM #APPLICANT [CTE]
		--LEFT JOIN Clients..STAGINGCUSTOMERVIEWHFC S ON CTE.CUID=S.SourceSystemCustomerCode   and [CTE].AppRefNumberforImages =s.AppRefNumberOfImages 
		--	AND S.SourceSystemName = 'HFC' --AND S.UPLOADSTATUS='N'
		WHERE [CTE].rn = 1 --and S.SourceSystemCustomerCode IS NULL --[CTE].[MotherFirstName] <> '' --AND ISNULL(CTE.CorrespondenceGlobalCity,'')<>'' AND ISNULL(CTE.PermanentCity,'')<>''
		) AS Final

	--Already uploaded for the in the Trackwizz
	UPDATE CKYC_RecordNotUploadReasonLog
	SET Reason = 'Record Already Uploaded in Trackwizz.'
	WHERE Cuid IN (
			SELECT Cuid
			FROM #UploadData
			)
		AND business = 'HFC'
		AND batchId = @batchId

	DELETE
	FROM Clients.dbo.StagingCustom217CustomerFlat
	WHERE SourceSystemCustomerCode IN (
			SELECT Cuid
			FROM #UploadData
			)
		AND SourceSystemName = 'HFC'
		AND batchId = @batchId

	SELECT DISTINCT SourceSystemCustomerCode
		,ApplicationRefNumber
	INTO #COCUSTOMER
	FROM Clients.dbo.StagingCustom217CustomerFlat WITH (NOLOCK)
	WHERE batchId = @batchId
		AND SourceSystemName = 'HFC'

	INSERT INTO DailyCoApplicantCustomerImageHFC (
		NAME
		,FILEPATH
		,FILETYPE
		,DOCUMENTTYPE
		,APPREFNOFORIMAGES
		,CUID
		,MKRID
		,BATCHID
		,ADDEDON
		,ISUPLOADED
		)
	SELECT NAME
		,filepath
		,filetype
		,documenttype
		,prospectNo AS AppRefNoForImages
		,cuid
		,mkrid
		,CONVERT(VARCHAR(100), @BatchId) AS BatchId
		,GETDATE() AS AddedOn
		,'N' AS IsUploaded
	FROM (
		SELECT srno
			,NAME
			,filepath
			,filetype
			,documenttype
			,PROSPECTNO
			,cuid
			,mkrid
		FROM (
			SELECT POA.srno
				,CASE 
					WHEN CHARINDEX('\', POA.filepath) > 0
						THEN REVERSE(SUBSTRING(Reverse(POA.filepath), 1, CHARINDEX('\', Reverse(POA.filepath)) - 1))
					ELSE ''
					END NAME
				,POA.filepath
				,lower(POA.fileextension) AS filetype
				,lower(POA.AttachmentCode) AS documenttype
				,POA.prospectNo
				,POA.SourceSysytemCustomerCode AS cuid
				,POA.mkrid
				,Row_number() OVER (
					PARTITION BY POA.SourceSysytemCustomerCode ORDER BY vw.[ProspectCreationDate] DESC
					) rn
			FROM #CTEDOCUMENTS_POA POA
			INNER JOIN #COCUSTOMER CC ON POA.PROSPECTNO = cc.ApplicationRefNumber
				AND CC.SourceSystemCustomerCode = POA.SOURCESYSYTEMCUSTOMERCODE
			INNER JOIN IILHFC.dbo.vw_AllApplicantsDetails vw WITH (NOLOCK) ON POA.PROSPECTNO = vw.PROSPECTNO
			) AS T
		WHERE RN < 2
		
		UNION
		
		SELECT SRNO
			,NAME
			,FILEPATH
			,FILETYPE
			,DOCUMENTTYPE
			,PROSPECTNO
			,CUID
			,MKRID
		FROM (
			SELECT A.srno
				,CASE 
					WHEN CHARINDEX('\', A.filepath) > 0
						THEN REVERSE(SUBSTRING(REVERSE(A.filepath), 1, CHARINDEX('\', Reverse(A.filepath)) - 1))
					ELSE ''
					END NAME
				,A.filepath
				,lower(A.fileextension) AS filetype
				,lower(A.AttachmentCode) AS documenttype
				,A.prospectNo
				,A.SourceSysytemCustomerCode AS cuid
				,A.mkrid
				,ROW_NUMBER() OVER (
					PARTITION BY A.SourceSysytemCustomerCode ORDER BY vw.ProspectCreationDate DESC
					) rn
			FROM #APPLICANTIMAGE A
			INNER JOIN #COCUSTOMER CC ON A.PROSPECTNO = cc.ApplicationRefNumber
				AND CC.SourceSystemCustomerCode = A.SOURCESYSYTEMCUSTOMERCODE
			INNER JOIN IILHFC.dbo.vw_AllApplicantsDetails vw WITH (NOLOCK) ON a.PROSPECTNO = vw.PROSPECTNO
			) AS TE
		WHERE RN < 2
		) AS F
	ORDER BY CUID

	SELECT SRNO
		,NAME
		,filepath
		,filetype
		,documenttype
		,AppRefNoForImages
		,cuid
		,mkrid
		,BatchId
	FROM DailyCoApplicantCustomerImageHFC WITH (NOLOCK)
	WHERE Batchid = @BatchId
		AND isuploaded = 'N'
END