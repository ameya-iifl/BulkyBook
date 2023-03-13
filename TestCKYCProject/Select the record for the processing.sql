--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
TEXT

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---- =============================================
---- Author:		Ravindra Sapkal
---- Create date: 3 May 2019
---- Description:	Get the photograph, POI, POA of PL
---- Changed on : 28 Feb 2020
---- Change Description:	commented on 28 Feb 2020 not required to check the id number for address proof
---- Change Description:	Change for including all the digital finance scheme
---- Change Description:	Change for CKYC Regulatory change
------ ============================================
/*
Author				Date		Remarks
--------------------------------------------------------------------------------------------------------------------------
Sameer Naik			05-07-2022	Changes for minimize rejection count.
Sameer Naik			13-07-2022	Add logs for record count
Sameer Naik			14-07-2022	Running data upload on daily basis as like SME.
Sameer Naik			18-07-2022	Added error log.
Sameer Naik			03-10-2022	Closed rejection reasons.
Sameer Naik			25-11-2022	Only one file will be selected for Address proof.
Ameya Mahale		15-12-2022	129763 - Removed dependency of smetab_documents
Shamita Das			10-01-2023	129763 - Removed dependency of smetab_documents
---EXEC proc_Daily_BYJUPL_CKYCCustomerAndImages
*/
CREATE PROC dbo.proc_Daily_BYJUPL_CKYCCustomerAndImages
AS
BEGIN
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	BEGIN TRY
		DECLARE @BatchId INT = 0

		--================================================================================
		-----added batch logic for the populating the data customer images table
		CREATE TABLE #tempBatch (
			STATUS VARCHAR(10)
			,BatchId INT
			)

		INSERT INTO #tempBatch (
			[STATUS]
			,BatchId
			)
		EXECUTE InsertUpdateBatchDetails 'PLBYJU'
			,0
			,'PLBYJU_Job'
			,0
			,0
			,0

		SELECT @BatchId = BatchId
		FROM #tempBatch
		WHERE STATUS = 0

		--================================================
		INSERT INTO dbo.CKYCDataUploadLog (
			processname
			,methodname
			,activity
			,CreatedOn
			,RecordsProcessed
			,BatchId
			,Response
			)
		VALUES (
			'DF Ckyc Data Upload'
			,'proc_Daily_BYJUPL_CKYCCustomerAndImages'
			,'Process-Start'
			,GETDATE()
			,0
			,@BatchId
			,'Process-Start'
			)

		DECLARE @FromDate VARCHAR(8) = CONVERT(VARCHAR(8), GETDATE() - 15, 112)
		DECLARE @CurrentDate VARCHAR(8) = CONVERT(VARCHAR(8), getdate(), 112)

		--create log table for all steps
		CREATE TABLE #Log (
			Activity VARCHAR(100)
			,RecordCount INT
			)

		IF OBJECT_ID('tempdb..#TEMP') IS NOT NULL
			DROP TABLE #TEMP

		IF OBJECT_ID('tempdb..#FinalImageList') IS NOT NULL
			DROP TABLE #FinalImageList

		SELECT DISTINCT M.prospectno
			,M.CUID
			,M.Mkrdt CreatedTime
			,branch
			,ClientStatus AS CustomerStatus
			,DC.IndvOrgMasterId AS IndvOrgMasterId
		INTO #TEMP
		FROM SME..tbl_ChequeDetails Ch WITH (NOLOCK)
		JOIN SME..tbl_clientmaster M WITH (NOLOCK) ON Ch.prospectno = M.prospectno
		INNER JOIN SME..PL_DocumentUpload DC WITH (NOLOCK) ON DC.ProspectNo = M.ProspectNo
			AND DC.CatID = '7'
		WHERE isdisbursed = 'Y' -- AND ch.HandoverDate  > @FromDate and   ch.HandoverDate < @CurrentDate
			AND CONVERT(VARCHAR(8), ch.HandoverDate, 112) = CONVERT(VARCHAR(8), GETDATE() - 1, 112)

		INSERT INTO #Log
		VALUES (
			'Jan makrdate and Apr hanovered cases of 85 and 89'
			,@@ROWCOUNT
			)

		--Rule any one should present MotherName/FathersName/SpouseName
		DELETE
		FROM #TEMP
		WHERE prospectno IN (
				SELECT DISTINCT T.prospectno
				FROM #TEMP T
				JOIN SME..INDIVIDUAL_MASTER I WITH (NOLOCK) ON T.prospectno = I.Ind_ProspectNo
				WHERE len(isnull(I.MotherName, '')) <= 2
					AND len(isnull(I.FathersName, '')) <= 2
					AND len(isnull(I.SpouseName, '')) <= 2
				)

		INSERT INTO #Log
		VALUES (
			'Records removed where MotherName/FathersName/SpouseName are blank'
			,@@ROWCOUNT
			)

		IF OBJECT_ID('tempdb..#TEMP4') IS NOT NULL
			DROP TABLE #TEMP4

		SELECT DISTINCT CUID
			,T.prospectno
			,ApplicantType
			,NAME
			,Mobile
			,Email
			,PANNo
			,IndvOrgMasterId
		INTO #TEMP4
		FROM #TEMP T
		JOIN SME..Indv_OrgMaster I WITH (NOLOCK) ON T.prospectno = I.prospectno
		WHERE G_COTYPE <> 'O';--ApplicantType <> 'APPLICANT' AND;

		WITH CTE
		AS (
			SELECT ROW_NUMBER() OVER (
					PARTITION BY NAME
					,ProspectNo
					,ApplicantType
					,NAME ORDER BY ProspectNo
						,ApplicantType
						,NAME
						,mobile DESC
					) AS SRNO
				,*
			FROM #TEMP4
			)
		DELETE
		FROM cte
		WHERE SRNO <> 1

		IF OBJECT_ID('tempdb..#TEMP1') IS NOT NULL
			DROP TABLE #TEMP1

		SELECT prospectno
			,APPTYPE
			,NAME
			,CUID
			,CUIDOriginal
			,CreatedTime
			,Title
			,MTitle
			,FTitle
			,fName
			,mName
			,LNAME
			,REPLACE(ISNULL(FathersName, ''), 'na', '') FathersName
			,DOB
			,REPLACE(ISNULL(SpouseName, ''), 'na', '') SpouseName
			,IsMarried
			,SEX
			,REPLACE(ISNULL(MotherName, ''), 'na', '') MotherName
			,Occupation
			,MkrId
			,branch
			,Mobile
			,Email
			,PANNo
			,AdharNo
			,CustomerStatus
		INTO #TEMP1
		FROM (
			SELECT T.prospectno
				,i.ApplicantType APPTYPE
				,I.name NAME
				,right('0000000000' + T.cuid, 10) + right('0000000000' + convert(VARCHAR, I.SRNO), 10) CUID
				,T.CUID AS CUIDOriginal
				,CreatedTime
				,Title
				,cm.MTitle
				,cm.FTitle
				,cm.fName
				,cm.mName
				,cm.LNAME
				,cm.FathersName
				,cm.DOB
				,cm.SpouseName
				,cm.IsMaried IsMarried
				,cm.SEX
				,cm.MotherName
				,'' Occupation
				,cm.Mkrid
				,cm.branch
				,i.Mobile Mobile
				,i.Email Email
				,i.PANNo PANNo
				,AdharNo
				,CustomerStatus
			FROM #TEMP T
			JOIN SME..Indv_OrgMaster I WITH (NOLOCK) ON T.prospectno = I.ProspectNo
				AND I.srno = T.IndvOrgMasterId
			JOIN #TEMP4 INDV ON I.ProspectNo = INDV.prospectno
				AND INDV.IndvOrgMasterId = I.srno
				AND i.ApplicantType = 'APPLICANT'
			INNER JOIN SME..tbl_clientmaster cm WITH (NOLOCK) ON cm.ProspectNo = i.ProspectNo
			
			UNION ALL
			
			SELECT T.prospectno
				,APPTYPE
				,fName + ' ' + mName + ' ' + lName NAME
				,right('0000000000' + T.cuid, 10) + right('0000000000' + convert(VARCHAR, I.SRNO), 10) CUID
				,T.CUID AS CUIDOriginal
				,CreatedTime
				,Title
				,MTitle
				,FTitle
				,fName
				,mName
				,LNAME
				,FathersName
				,DOB
				,SpouseName
				,IsMarried
				,SEX
				,MotherName
				,Occupation
				,I.MkrId
				,branch
				,Mobile
				,Email
				,PANNo
				,Adharno
				,CustomerStatus
			FROM #TEMP T
			JOIN SME..INDIVIDUAL_MASTER I WITH (NOLOCK) ON T.prospectno = I.Ind_ProspectNo
				AND I.srno = T.IndvOrgMasterId
			JOIN #TEMP4 INDV ON I.Ind_ProspectNo = INDV.prospectno
				AND INDV.IndvOrgMasterId = I.srno
				AND ApplicantType <> 'APPLICANT'
			) tmp

		INSERT INTO #Log
		VALUES (
			'Distinct individuals present'
			,@@ROWCOUNT
			)

		DROP TABLE #TEMP4;

		WITH CTE
		AS (
			SELECT ROW_NUMBER() OVER (
					PARTITION BY CUID ORDER BY ProspectNo
						,AppType
						,NAME
						,mobile DESC
					) AS SRNO
				,*
			FROM #TEMP1
			)
		DELETE
		FROM CTE
		WHERE SRNO <> 1

		IF (@@ROWCOUNT > 0)
			INSERT INTO #Log
			VALUES (
				'Duplicate records removed'
				,@@ROWCOUNT
				);

		--DROP TABLE #TEMP 
		IF OBJECT_ID('tempdb..#TEMP2') IS NOT NULL
			DROP TABLE #TEMP2

		SELECT '1' AS SRNO
			,P.CUID
			,CUIDOriginal
			,CreatedTime
			,P.Title
			,P.MTitle
			,P.FTitle
			,P.fName
			,P.mName
			,P.LNAME
			,P.FathersName
			,P.DOB
			,P.SpouseName
			,P.IsMarried
			,P.SEX
			,P.MotherName
			,P.Occupation
			,P.MkrId
			,P.branch
			,P.Mobile
			,P.Email
			,P.PANNo
			,P.AdharNo
			,doc.ProspectNo
			,APPTYPE
			,dbo.FN_GetApplicantName(P.fName, P.mName, P.LNAME) Applicant_Name
			,'Address Proof' ParametersName
			,sc.SubCatName ParameterName
			,Extension File_Extension
			,DocumentPath Image_Path
			,CASE 
				WHEN sc.SubCatName = 'Aadhar'
					THEN 1
				WHEN sc.SubCatName = 'Aadhar Back'
					THEN 2
				WHEN sc.SubCatName = 'Voters ID'
					THEN 3
				WHEN sc.SubCatName = 'Passport'
					THEN 4
				ELSE 5
				END IDProof
			,CASE 
				WHEN sc.SubCatName = 'Aadhar'
					THEN Convert(VARCHAR(50), cm.AdharNo)
				WHEN sc.SubCatName = 'Aadhar Back'
					THEN Convert(VARCHAR(50), cm.AdharNo)
				WHEN sc.SubCatName = 'Voters ID'
					THEN Convert(VARCHAR(50), cm.VoterID)
				WHEN sc.SubCatName = 'Passport'
					THEN Convert(VARCHAR(50), cm.PassportNo)
				ELSE ''
				END IDNumber
			,CustomerStatus
			,FORMAT(CONVERT(DATETIME, CreatedTime, 6), 'dd-MMM-yyyy') AS CustomerStatusEffectiveDate
		INTO #TEMP2
		FROM #TEMP1 P WITH (NOLOCK)
		INNER JOIN SME..PL_DocumentUpload doc WITH (NOLOCK) ON P.prospectno = doc.prospectno
		INNER JOIN SME..tbl_ClientMaster cm WITH (NOLOCK) ON cm.ProspectNo = doc.ProspectNo
		INNER JOIN SME..pl_subcategorymaster sc WITH (NOLOCK) ON doc.SubCatID = CAST(sc.SubCatID AS VARCHAR(10))
		INNER JOIN SME..pl_categorymaster cat WITH (NOLOCK) ON cat.CategoryID = sc.CategoryID
			AND doc.CatID = cat.CategoryID
		WHERE cm.Portfolio IN (
				'85'
				,'89'
				)
			AND doc.CatID = 2
			AND Isnull(DocumentPath, '') <> ''
		--union all
		--Select  
		--	'2' as SRNO,P.CUID ,CUIDOriginal,CreatedTime,P.Title,P.MTitle,P.FTitle ,P.fName , P.mName , P.LNAME ,P.FathersName , P.DOB, P.SpouseName , P.IsMarried , P.SEX ,P.MotherName  ,P.Occupation 
		--	,P.MkrId,P.branch,P.Mobile ,P.Email ,P.PANNo ,P.AdharNo,
		--    doc.ProspectNo
		--	,APPTYPE
		--	,dbo.FN_GetApplicantName(P.fName,P.mName,P.LNAME) Applicant_Name
		--	,'Identity Proof'  ParametersName,sc.SubCatName ParameterName
		--	,Extension File_Extension
		--	,DocumentPath Image_Path
		--	,CASE 
		--	WHEN sc.SubCatName = 'PAN'
		--		THEN 6
		--	WHEN  sc.SubCatName = 'Aadhar'
		--		THEN 7
		--	WHEN sc.SubCatName = 'Voters ID'
		--		THEN 8
		--	WHEN sc.SubCatName = 'Passport'
		--		THEN 9
		--	ELSE 10
		--	END IDProof 
		--	,CASE 
		--	WHEN sc.SubCatName = 'PAN'
		--		THEN Convert(Varchar(50),cm.PANNo)
		--	WHEN  sc.SubCatName = 'Aadhar'
		--		THEN Convert(Varchar(50),cm.AdharNo)
		--	WHEN sc.SubCatName = 'Voters ID'
		--		THEN Convert(Varchar(50),cm.VoterID)
		--	WHEN sc.SubCatName = 'Passport'
		--		THEN Convert(Varchar(50),cm.PassportNo)
		--	ELSE ''
		--	END IDNumber 
		--from #TEMP1 P WITH (NOLOCK)	
		--	inner join SME..PL_DocumentUpload doc WITH (NOLOCK)	
		--		ON P.prospectno = doc.prospectno 
		--	inner join SME..tbl_ClientMaster cm
		--		on cm.ProspectNo = doc.ProspectNo
		--	Inner Join SME..pl_subcategorymaster sc
		--		on doc.SubCatID = sc.SubCatID
		--	Inner Join SME..pl_categorymaster cat with(nolock)
		--		on cat.CategoryID = sc.CategoryID and doc.CatID = cat.CategoryID
		--Where cm.Portfolio in ('85','89') and doc.CatID = 1 and Isnull(DocumentPath,'') <> ''
		
		UNION ALL
		
		SELECT '3' AS SRNO
			,P.CUID
			,CUIDOriginal
			,CreatedTime
			,P.Title
			,P.MTitle
			,P.FTitle
			,P.fName
			,P.mName
			,P.LNAME
			,P.FathersName
			,P.DOB
			,P.SpouseName
			,P.IsMarried
			,P.SEX
			,P.MotherName
			,P.Occupation
			,P.MkrId
			,P.branch
			,P.Mobile
			,P.Email
			,P.PANNo
			,P.AdharNo
			,doc.ProspectNo
			,APPTYPE
			,dbo.FN_GetApplicantName(P.fName, P.mName, P.LNAME) Applicant_Name
			--,sc.SubCatName  ParametersName,sc.SubCatName ParameterName
			,'Photograph' ParametersName
			,'Photograph' ParameterName
			,Extension File_Extension
			,DocumentPath Image_Path
			,CASE 
				WHEN sc.SubCatName = 'Selfile'
					THEN 11
				ELSE 12
				END IDProof
			,'Photograph' IDNumber
			,CustomerStatus
			,FORMAT(CONVERT(DATETIME, CreatedTime, 6), 'dd-MMM-yyyy') AS CustomerStatusEffectiveDate
		FROM #TEMP1 P WITH (NOLOCK)
		INNER JOIN SME..PL_DocumentUpload doc WITH (NOLOCK) ON P.prospectno = doc.prospectno
		INNER JOIN SME..tbl_ClientMaster cm WITH (NOLOCK) ON cm.ProspectNo = doc.ProspectNo
		INNER JOIN SME..pl_subcategorymaster sc WITH (NOLOCK) ON doc.SubCatID = CAST(sc.SubCatID AS VARCHAR(10))
		INNER JOIN SME..pl_categorymaster cat WITH (NOLOCK) ON cat.CategoryID = sc.CategoryID
			AND doc.CatID = cat.CategoryID
		WHERE cm.Portfolio IN (
				'85'
				,'89'
				)
			AND doc.CatID = 7
			AND Isnull(DocumentPath, '') <> '';

		--Change Description:	commented on 28 Feb 2020 not required to check the id number for address proof
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'Aadhar' and IDProof = 1 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'Aadhar' and IDProof = 1 and Isnull(IDNumber,'') = ''
		--	INSERT INTO @Log VALUES ('Address Proof delete due to Aadhar not present', @@ROWCOUNT)
		--End
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'Aadhar Back' and IDProof = 2 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'Aadhar Back' and IDProof = 2 and Isnull(IDNumber,'') = '' 
		--	INSERT INTO @Log VALUES ('Address Proof delete due to Aadhar Back not present', @@ROWCOUNT)
		--End
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'Voters ID' and IDProof = 3 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'Voters ID' and IDProof = 3 and Isnull(IDNumber,'') = ''
		--	INSERT INTO @Log VALUES ('Address Proof delete due to Voters ID not present', @@ROWCOUNT)
		--End
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'Passport' and IDProof = 4 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'Passport' and IDProof = 4 and Isnull(IDNumber,'') = ''
		--	INSERT INTO @Log VALUES ('Address Proof delete due to Passport not present', @@ROWCOUNT)
		--End
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'PAN' and IDProof = 6 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'PAN' and IDProof = 6 and Isnull(IDNumber,'') = ''
		--	INSERT INTO @Log VALUES ('Identity Proof delete due to PAN not present', @@ROWCOUNT)
		--End
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'Aadhar' and IDProof = 7 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'Aadhar' and IDProof = 7 and Isnull(IDNumber,'') = ''
		--	INSERT INTO @Log VALUES ('Identity Proof delete due to Aadhar not present', @@ROWCOUNT)
		--End
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'Voters ID' and IDProof = 8 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'Voters ID' and IDProof = 8 and Isnull(IDNumber,'') = ''
		--	INSERT INTO @Log VALUES ('Identity Proof delete due to Voters ID not present', @@ROWCOUNT)
		--End
		--If Exists(Select Top 1 1 From #TEMP2 where ParameterName = 'Passport' and IDProof = 9 and Isnull(IDNumber,'') = '')
		--Begin
		--	Delete  from #TEMP2 where ParameterName = 'Passport' and IDProof = 9 and Isnull(IDNumber,'') = ''
		--	INSERT INTO @Log VALUES ('Identity Proof delete due to Passport not present', @@ROWCOUNT)
		--End
		--Delete  from #TEMP2 Where Isnull(IDNumber,'') = ''
		---removed when both images are ot present
		DELETE
		FROM #TEMP1
		WHERE CUID IN (
				SELECT T1.CUID
				FROM #TEMP1 T1
				LEFT JOIN #TEMP2 T2 ON T1.CUID = T2.CUID
				WHERE T2.CUID IS NULL
				)

		INSERT INTO #Log
		VALUES (
			'Records removed where POA and Photo all 2 images not present'
			,@@ROWCOUNT
			);

		IF OBJECT_ID('tempdb..#TEMP6') IS NOT NULL
			DROP TABLE #TEMP6

		---removed when both images are ot present
		SELECT DISTINCT CUID
		INTO #TEMP6
		FROM #TEMP2
		GROUP BY CUID
		HAVING count(cuid) < 2

		INSERT INTO #Log
		VALUES (
			'Records removed where POA or photo images not present'
			,@@ROWCOUNT
			);

		DELETE
		FROM #TEMP2
		WHERE CUID IN (
				SELECT CUID
				FROM #TEMP6
				);

		--DROP TABLE #TEMP6
		IF OBJECT_ID('tempdb..#TEMP3') IS NOT NULL
			DROP TABLE #TEMP3

		SELECT *
			,(
				SELECT FirstName
				FROM MasterHub.dbo.SplitName(FathersName)
				) AS FatherFirstName
			,(
				SELECT MiddleName
				FROM MasterHub.dbo.SplitName(FathersName)
				) AS FatherMiddleName
			,(
				SELECT LastName
				FROM MasterHub.dbo.SplitName(FathersName)
				) AS FatherLastName
			,(
				SELECT FirstName
				FROM MasterHub.dbo.SplitName(SpouseName)
				) AS SpouseFirstName
			,(
				SELECT MiddleName
				FROM MasterHub.dbo.SplitName(SpouseName)
				) AS SpouseMiddleName
			,(
				SELECT LastName
				FROM MasterHub.dbo.SplitName(SpouseName)
				) AS SpouseLastName
			,(
				SELECT FirstName
				FROM MasterHub.dbo.SplitName(MotherName)
				) AS MotherFirstName
			,(
				SELECT MiddleName
				FROM MasterHub.dbo.SplitName(MotherName)
				) AS MotherMiddleName
			,(
				SELECT LastName
				FROM MasterHub.dbo.SplitName(MotherName)
				) AS MotherLastName
			,CONVERT(VARCHAR(100), '') AS AddressProof
			,CONVERT(VARCHAR(100), '') AS IdentityProof
			,CONVERT(VARCHAR(100), '') AS PhotoProof
		INTO #TEMP3
		FROM #TEMP2
		ORDER BY ProspectNo
			,AppType
			,Applicant_Name
			,ParametersName
			,ParameterName
			,IDProof

		--Select '#temp3',* from #TEMP3
		--DROP TABLE #TEMP2
		UPDATE T1
		SET T1.AddressProof = T1.ParameterName
		FROM #TEMP3 T1
		WHERE T1.ParametersName = 'Address Proof'

		--UPDATE T1 SET T1.IdentityProof = T1.ParameterName 
		--from #TEMP3 T1 
		--WHERE T1.ParametersName ='Identity Proof'
		UPDATE T1
		SET T1.PhotoProof = T1.ParameterName
		FROM #TEMP3 T1
		WHERE T1.ParametersName = 'Photograph';

		IF OBJECT_ID('tempdb..#TEMP5') IS NOT NULL
			DROP TABLE #TEMP5

		SELECT ROW_NUMBER() OVER (
				PARTITION BY [SourcesystemCustCode] ORDER BY RN DESC
				) AS RN
			,SourceSystemName
			,SourcesystemCustCode
			,SmallCustomer
			,EkycOTPbased
			,SourceSystemCustomerCreationDate
			,ConstitutionType
			,Prefix
			,FirstName
			,MiddleName
			,LastName
			,MaidenPrefix
			,MaidenFirstName
			,MaidenMiddleName
			,MaidenLastName
			,CASE 
				WHEN isnull(FatherPrefix, '') = ''
					AND isnull(FatherFirstName, '') != ''
					THEN 'Mr'
				ELSE FatherPrefix
				END FatherPrefix
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
			,OccupationType
			,DateofBirth
			,ResidentialStatus
			,EmailId
			,KYCDateOfDeclaration
			,KYCPlaceOfDeclaration
			,KYCVerificationDate
			,KYCEmployeeName
			,KYCEmployeeDesignation
			,KYCVerificationBranch
			,KYCEmployeeCode
			,PermanentCKYCAddType
			,PermanentCountry
			,PermanentPin
			,PermanentAddressLine1
			,PermanentAddressLine2
			,PermanentAddressLine3
			,PermanentDistrict
			,PermanentCity
			,PermanentState
			,AddressProof
			,PermanentAddressProof
			,CorrespondenceGlobalCountry
			,CorrespondenceGlobalPin
			,CorrespondenceGlobalAddressLine1
			,CorrespondenceGlobalAddressLine2
			,CorrespondenceGlobalAddressLine3
			,CorrespondenceGlobalDistrict
			,CorrespondenceGlobalCity
			,CorrespondenceGlobalState
			,JurisdictionOfResidence
			,CountryOfBirth
			,BirthCity
			,TaxIdentificationNumber
			,TaxResidencyAddressLine1
			,TaxResidencyAddressLine2
			,TaxResidencyAddressLine3
			,TaxResidencyPin
			,TaxResidencyDistrict
			,TaxResidencyCity
			,TaxResidencyState
			,TaxResidencyCountry
			,ResidentialSTDCode
			,ResidentialTelephoneNumber
			,OfficeSTDCode
			,OfficeTelephoneNumber
			,MobileISD
			,MobileNumber
			,FaxSTD
			,FaxNumber
			,CKYCID
			,PassportExpiryDate
			,VoterIdCard
			,PAN
			,DrivingLicenseNumber
			,DrivingLicenseExpiryDate
			,Aadhaar
			,NREGA
			,CKYCPOIOtherCentralGovtID
			,CKYCPOIS01IDNumber
			,CKYCPOIS02IDNumber
			,ProofOfIDSubmitted
			,CustomerDemiseDate
			,Minor
			,SourcesystemRelatedPartyCode
			,RelatedPersonType
			,RelatedPersonPrefix
			,RelatedPersonFirstName
			,RelatedPersonMiddleName
			,RelatedPersonLastName
			,RelatedPersonCKYCID
			,RelatedPersonPassportNumber
			,RelatedPersonPassportExpiryDate
			,RelatedPersonVoterIdCard
			,RelatedPersonPAN
			,RelatedPersonDrivingLicenseNumber
			,RelatedPersonDrivingLicenseExpiryDate
			,RelatedPersonAadhaar
			,RelatedPersonNREGA
			,RelatedPersonCKYCPOIOtherCentralGovtID
			,RelatedPersonCKYCPOIS01IDNumber
			,RelatedPersonCKYCPOIS02IDNumber
			,RelatedPersonProofOfIDSubmitted
			,SourceSystemSegment
			,AppRefNumberforImages
			,HolderforImages
			,BranchCode
			,IsValid
			,CustomerStatusEffectiveDate
			,CustomerStatus
		INTO #TEMP5
		FROM (
			SELECT DISTINCT 'PLBYJU' AS [SourceSystemName]
				,T.CUID AS [SourcesystemCustCode]
				,0 AS [SmallCustomer]
				,0 AS [EkycOTPbased]
				,CASE 
					WHEN ISDATE(t.[CreatedTime]) = 1
						THEN FORMAT(CONVERT(DATETIME, t.[CreatedTime], 6), 'dd-MMM-yyyy')
					ELSE NULL
					END AS [SourceSystemCustomerCreationDate]
				,'1' AS [ConstitutionType]
				,t.Title [Prefix]
				,t.fname [FirstName]
				,t.mName [MiddleName]
				,t.LNAME [LastName]
				,'' [MaidenPrefix]
				,'' [MaidenFirstName]
				,'' [MaidenMiddleName]
				,'' [MaidenLastName]
				,CASE 
					WHEN ISNULL(t.FathersName, '') <> ''
						THEN (
								CASE 
									WHEN isnull(t.FTitle, '') <> ''
										THEN FTitleMaster.Name
									ELSE 'Mr'
									END
								)
					ELSE ''
					END [FatherPrefix]
				,REPLACE(t.FatherFirstName, '.', ' ') AS [FatherFirstName]
				,t.FatherMiddleName
				,t.FatherLastName
				,CASE 
					WHEN ISNULL(t.[SpouseFirstName], '') <> ''
						THEN (
								CASE 
									WHEN t.[Sex] = 'M'
										THEN 'Mrs'
									ELSE 'Mr'
									END
								)
					ELSE ''
					END AS [SpousePrefix]
				,t.SpouseFirstName
				,t.SpouseMiddleName
				,t.SpouseLastName
				,CASE 
					WHEN ISNULL(t.[MotherFirstName], '') <> ''
						THEN (
								CASE 
									WHEN isnull(t.MTitle, '') <> ''
										THEN MTitleMaster.Name
									ELSE 'Mrs'
									END
								)
					ELSE ''
					END AS [MotherPrefix]
				,REPLACE(t.[MotherFirstName], '.', '') AS [MotherFirstName]
				,t.MotherMiddleName
				,t.MotherLastName
				,t.[Sex] AS [Gender]
				,CASE 
					WHEN t.IsMarried = 'Married'
						THEN 'M'
					WHEN t.IsMarried = 'Single'
						THEN 'U'
					ELSE 'O'
					END AS [MaritalStatus]
				,'IN' AS [Citizenship]
				,CASE 
					WHEN t.Occupation IN ('Business')
						THEN 'B-01'
					WHEN t.Occupation IN ('Others')
						THEN 'O-02'
					WHEN t.Occupation IN ('SERVICE')
						THEN 'S-02'
					ELSE 'X-01'
					END AS [OccupationType]
				,CASE 
					WHEN ISDATE(T.DOB) = 1
						THEN FORMAT(CONVERT(DATETIME, T.DOB), 'dd-MMM-yyyy')
					ELSE NULL
					END AS [DateofBirth]
				,'01' AS [ResidentialStatus]
				,'' [EmailId]
				,CASE 
					WHEN ISDATE(t.[CreatedTime]) = 1
						THEN FORMAT(CONVERT(DATETIME, t.[CreatedTime], 6), 'dd-MMM-yyyy')
					ELSE NULL
					END AS [KYCDateOfDeclaration]
				,T.branch AS [KYCPlaceOfDeclaration]
				,CASE 
					WHEN ISDATE(t.[CreatedTime]) = 1
						THEN FORMAT(CONVERT(DATETIME, t.[CreatedTime], 6), 'dd-MMM-yyyy')
					ELSE NULL
					END AS [KYCVerificationDate]
				,ISNULL(COALESCE(emp.EmpName, ''), '') AS [KYCEmployeeName]
				,ISNULL(COALESCE(emp.Designation, ''), '') AS [KYCEmployeeDesignation]
				,'HO' AS [KYCVerificationBranch]
				,T.MKRID AS [KYCEmployeeCode]
				,'01' AS [PermanentCKYCAddType]
				,'IN' AS [PermanentCountry]
				,COALESCE(CA.AD_PinCode, CA1.AD_PinCode) AS [PermanentPin]
				,COALESCE(CA.ad_Add1, CA1.ad_Add1) AS [PermanentAddressLine1]
				,COALESCE(CA.ad_Add2, CA1.ad_Add2) AS [PermanentAddressLine2]
				,COALESCE(CA.ad_Add3, CA1.ad_Add3) AS [PermanentAddressLine3]
				,'' AS [PermanentDistrict]
				,ISNULL((
						CASE 
							WHEN ISNULL(cm.City_Description, '') <> ''
								THEN cm.City_Description
							ELSE cm1.City_Description
							END
						), '') AS [PermanentCity]
				,CASE 
					WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'CT'
						THEN 'CG'
					WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'TG'
						THEN 'TS'
					WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'UT'
						THEN 'UA'
					ELSE ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '')
					END AS [PermanentState]
				,t3.AddressProof ----> del
				,(
					CASE 
						WHEN t3.AddressProof = 'PASSPORT'
							THEN 'Passport'
						WHEN t3.AddressProof = 'DRIVING LICENSE'
							THEN 'DrivingLicence'
						WHEN t3.AddressProof = 'Voter ID'
							THEN 'VoterID'
						WHEN t3.AddressProof = 'RATION CARD'
							THEN 'RationCard'
						WHEN t3.AddressProof = 'LIC POLICY OR RECEIPT'
							THEN 'OthersPOACKYCInd'
						WHEN t3.AddressProof = 'LEAVE AND LICENSE AGREEMENT'
							THEN 'OthersPOACKYCInd'
						WHEN t3.AddressProof = 'GOVERNMENT ID CARD'
							THEN 'OthersPOACKYCInd'
						WHEN t3.AddressProof = 'UTILITY BILL'
							THEN 'Utilitybill2m'
						WHEN t3.AddressProof = 'Aadhar'
							THEN 'AadharCard'
						WHEN t3.AddressProof = 'Aadhar Back'
							THEN 'AadharCard'
						ELSE t3.AddressProof
						END
					) AS [PermanentAddressProof]
				,'IN' AS [CorrespondenceGlobalCountry]
				,COALESCE(CA.AD_PinCode, CA1.AD_PinCode) AS [CorrespondenceGlobalPin]
				,COALESCE(CA.ad_Add1, CA1.ad_Add1) AS [CorrespondenceGlobalAddressLine1]
				,COALESCE(CA.ad_Add2, CA1.ad_Add2) AS [CorrespondenceGlobalAddressLine2]
				,COALESCE(CA.ad_Add3, CA1.ad_Add3) AS [CorrespondenceGlobalAddressLine3]
				,'' AS [CorrespondenceGlobalDistrict]
				,ISNULL((COALESCE(cm.City_Description, cm1.City_Description, '')), '') AS [CorrespondenceGlobalCity]
				,CASE 
					WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'CT'
						THEN 'CG'
					WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'TG'
						THEN 'TS'
					WHEN ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '') = 'UT'
						THEN 'UA'
					ELSE ISNULL(COALESCE(SM.State_Code, SM1.State_Code), '')
					END AS [CorrespondenceGlobalState]
				,'IN' AS [JurisdictionOfResidence]
				,'IN' AS [CountryOfBirth]
				,'' AS [BirthCity]
				,'' AS [TaxIdentificationNumber]
				,'' AS [TaxResidencyAddressLine1]
				,'' AS [TaxResidencyAddressLine2]
				,'' AS [TaxResidencyAddressLine3]
				,'' AS [TaxResidencyPin]
				,'' AS [TaxResidencyDistrict]
				,ISNULL((COALESCE(cm.City_Description, cm1.City_Description)), '') AS [TaxResidencyCity]
				,'' AS [TaxResidencyState]
				,'IN' AS [TaxResidencyCountry]
				,NULL AS [ResidentialSTDCode]
				,NULL AS [ResidentialTelephoneNumber]
				,NULL AS [OfficeSTDCode]
				,NULL AS [OfficeTelephoneNumber]
				,NULL AS [MobileISD]
				,CASE 
					WHEN ISNUMERIC(LEFT(t.[Mobile], 10)) = 1
						THEN LEFT(t.[Mobile], 10)
					ELSE NULL
					END AS [MobileNumber]
				,NULL AS [FaxSTD]
				,NULL AS [FaxNumber]
				,NULL AS [CKYCID]
				,NULL AS [PassportExpiryDate]
				,'' AS [VoterIdCard]
				,t.PANNo AS [PAN]
				,'' AS [DrivingLicenseNumber]
				,NULL AS [DrivingLicenseExpiryDate]
				,T.AdharNo AS [Aadhaar]
				,'' AS [NREGA]
				,'' AS [CKYCPOIOtherCentralGovtID]
				,'' AS [CKYCPOIS01IDNumber]
				,'' AS [CKYCPOIS02IDNumber]
				,'' AS [ProofOfIDSubmitted]
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
				,t.ProspectNo AS [AppRefNumberforImages]
				,'' AS [HolderforImages]
				,T.branch BranchCode
				,T.CustomerStatus
				,T.CustomerStatusEffectiveDate
				,CASE 
					WHEN ISNULL(t.FatherFirstName, '') <> ''
						OR ISNULL(t.SpouseFirstName, '') <> ''
						THEN 'Y'
					ELSE 'N'
					END AS [IsValid]
				,T.CUID
				,DENSE_RANK() OVER (
					PARTITION BY T.CUID ORDER BY T.CUID DESC
					) AS RN
			FROM #TEMP3 T
			LEFT OUTER JOIN [UCID].[dbo].[Unique_Customers] [U] WITH (NOLOCK) ON [T].[CUIDOriginal] = [U].[CustomerID]
				AND (
					Business = 'SME'
					OR Business = 'PL'
					OR business = 'Digital Finance'
					)
			--[atom.iifl.in].[UCID].[dbo].[Unique_Customers] AS [U] ON [APP].[CUID] = [U].[CustomerID]
			LEFT OUTER JOIN [Masterhub].[dbo].[EmployeeDataAll] [Emp] WITH (NOLOCK) ON [T].[Mkrid] = [Emp].[EmpId]
			LEFT JOIN SME.dbo.tbl_clientaddresses CA WITH (NOLOCK) ON CA.AD_PROSPECTNO = PROSPECTNO
				AND APPTYPE = CA.ApplicantType
				AND CA.ApplicantName = Applicant_Name
				AND ca.ad_AddressType = 'PERMANENT RESIDENCE'
			LEFT JOIN SME.dbo.tbl_clientaddresses CA1 WITH (NOLOCK) ON CA1.AD_PROSPECTNO = PROSPECTNO
				AND APPTYPE = CA1.ApplicantType
				AND CA1.ApplicantName = Applicant_Name
				AND CA1.ad_AddressType = 'CURRENT RESIDENCE'
			LEFT JOIN SME.dbo.City_Master cm WITH (NOLOCK) ON cm.City_Code = CA.ad_City
				OR cm.CityId = CA.ad_City
			LEFT JOIN SME.dbo.state_master SM WITH (NOLOCK) ON SM.State_Code = CA.ad_State
			LEFT JOIN SME.dbo.City_Master cm1 WITH (NOLOCK) ON cm1.City_Code = CA1.ad_City
				OR cm1.CityId = CA1.ad_City
			LEFT JOIN SME.dbo.state_master SM1 WITH (NOLOCK) ON SM1.State_Code = CA1.ad_State
			LEFT JOIN Masterhub..LOV FTitleMaster WITH (NOLOCK) ON FTitleMaster.Value = T.FTitle
				AND FTitleMaster.ListType = 'Title'
			LEFT JOIN Masterhub..LOV MTitleMaster WITH (NOLOCK) ON MTitleMaster.Value = T.MTitle
				AND MTitleMaster.ListType = 'Title'
			LEFT JOIN #TEMP3 T3 ON T.Prospectno = t3.prospectno
				AND T3.SRNO = 1
			--LEFT JOIN #TEMP3 T4 ON T.Prospectno = t4.prospectno and T4.SRNO = 2 
			LEFT JOIN #TEMP3 T5 ON T.Prospectno = t5.prospectno
				AND T5.SRNO = 3
				--LEFT JOIN #UploadedData S124 with (Nolock) on T.CUID =s124.SourceSystemCustomerCode
				--WHERE s124.SourceSystemCustomerCode is null 
			) TMP

		INSERT INTO #Log
		VALUES (
			'Data records for CERSAI'
			,@@ROWCOUNT
			)

		INSERT INTO StagingCustom217CustomerFlat (
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
		SELECT 'IIFL 1' AS ParentCompany
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
			,CASE 
				WHEN [PermanentAddressProof] LIKE '%aadhar%'
					THEN 'AadharCard'
				WHEN [PermanentAddressProof] LIKE '%VOTER%'
					THEN 'VoterId'
				WHEN [PermanentAddressProof] LIKE '%Driv%'
					THEN 'DrivingLicence'
				WHEN [PermanentAddressProof] LIKE '%Passport%'
					THEN 'Passport'
				ELSE [PermanentAddressProof]
				END [PermanentAddressProof]
			,[CorrespondenceGlobalCountry] AS CorrespondenceAddressCountry
			,[CorrespondenceGlobalPin] AS CorrespondenceAddressPinCode
			,Final.CorrespondenceGlobalAddressLine1 AS [CorrespondenceAddressLine1]
			,Final.CorrespondenceGlobalAddressLine2 AS [CorrespondenceAddressLine2]
			,Final.CorrespondenceGlobalAddressLine3 AS [CorrespondenceAddressLine3]
			,[CorrespondenceGlobalDistrict] AS CorrespondenceAddressDistrict
			,[CorrespondenceGlobalCity] AS CorrespondenceAddressCity
			,[CorrespondenceGlobalState] AS CorrespondenceAddressState
			,CASE 
				WHEN [PermanentAddressProof] LIKE '%aadhar%'
					THEN 'AadharCard'
				WHEN [PermanentAddressProof] LIKE '%VOTER%'
					THEN 'VoterId'
				WHEN [PermanentAddressProof] LIKE '%Driv%'
					THEN 'DrivingLicence'
				WHEN [PermanentAddressProof] LIKE '%Passport%'
					THEN 'Passport'
				ELSE [PermanentAddressProof]
				END CorrespondenceAddressProof
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
			,Passport AS PassportNumber
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
			,0 AS [Minor]
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
			,0 AS FORMSixty
			,'' AS NPRLetter
			,'01' AS KYCAttestationType
			,@batchId AS batchId
			,'Y' AS IsValid
		FROM (
			SELECT 'PLBYJU' + CONVERT(VARCHAR(10), GETDATE(), 112) + CONVERT(VARCHAR(2), DATEPART(HOUR, GETDATE())) + CONVERT(VARCHAR(2), DATEPART(MINUTE, GETDATE())) + RIGHT('0000000' + CONVERT(VARCHAR(10), ROW_NUMBER() OVER (
							ORDER BY [SourcesystemCustCode]
							)), 10) AS [TransactionID]
				,[SourceSystemName]
				,SourcesystemCustCode
				,Final.SmallCustomer AS [IsSmallCustomer]
				,[EkycOTPbased]
				,[SourceSystemCustomerCreationDate]
				,[ConstitutionType]
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
				,[OccupationType]
				,[DateofBirth]
				,[ResidentialStatus]
				,[EmailId]
				,[KYCDateOfDeclaration]
				,[KYCPlaceOfDeclaration]
				,[KYCVerificationDate]
				,'Byjus' [KYCEmployeeName]
				,'Vendor' [KYCEmployeeDesignation]
				,[KYCVerificationBranch]
				,[KYCEmployeeCode]
				,Final.PermanentCKYCAddType
				,[PermanentCountry]
				,[PermanentPin]
				,[PermanentAddressLine1]
				,[PermanentAddressLine2]
				,[PermanentAddressLine3]
				,[PermanentDistrict]
				,[PermanentCity]
				,[PermanentState]
				,[PermanentAddressProof]
				,[CorrespondenceGlobalCountry]
				,[CorrespondenceGlobalPin]
				,Final.CorrespondenceGlobalAddressLine1
				,Final.CorrespondenceGlobalAddressLine2
				,Final.CorrespondenceGlobalAddressLine3
				,[CorrespondenceGlobalDistrict]
				,[CorrespondenceGlobalCity]
				,[CorrespondenceGlobalState]
				,[JurisdictionOfResidence]
				,[CountryOfBirth]
				,[BirthCity]
				,[TaxIdentificationNumber]
				,[TaxResidencyAddressLine1]
				,[TaxResidencyAddressLine2]
				,[TaxResidencyAddressLine3]
				,[TaxResidencyPin]
				,[TaxResidencyDistrict]
				,[TaxResidencyCity]
				,[TaxResidencyState]
				,[TaxResidencyCountry]
				,[ResidentialSTDCode]
				,[ResidentialTelephoneNumber]
				,[OfficeSTDCode]
				,[OfficeTelephoneNumber]
				,[MobileISD]
				,[MobileNumber]
				,[FaxSTD]
				,[FaxNumber]
				,Final.CKYCID
				,'' AS [Passport]
				,[PassportExpiryDate]
				,[VoterIdCard]
				,[PAN]
				,[DrivingLicenseNumber]
				,[DrivingLicenseExpiryDate]
				,CASE 
					WHEN ISNULL(Aadhaar, '') = ''
						THEN '000000001234'
					ELSE Aadhaar
					END [Aadhaar]
				,[NREGA]
				,[CKYCPOIOtherCentralGovtID]
				,[CKYCPOIS01IDNumber]
				,[CKYCPOIS02IDNumber]
				,[ProofOfIDSubmitted]
				,[CustomerDemiseDate]
				,Final.Minor
				,'' AS [SourcesystemRelatedPartyCode]
				,[RelatedPersonType]
				,[RelatedPersonPrefix]
				,[RelatedPersonFirstName]
				,[RelatedPersonMiddleName]
				,[RelatedPersonLastName]
				,Final.RelatedPersonCKYCID AS [RelatedPersonCKYCNumber]
				,[RelatedPersonPassportNumber]
				,[RelatedPersonPassportExpiryDate]
				,[RelatedPersonVoterIdCard]
				,[RelatedPersonPAN]
				,RelatedPersonDrivingLicenseNumber AS [RelatedPersonDrivingLicenseNnumber]
				,[RelatedPersonDrivingLicenseExpiryDate]
				,[RelatedPersonAadhaar]
				,[RelatedPersonNREGA]
				,final.RelatedPersonCKYCPOIOtherCentralGovtID AS [RelatedPersonCKYCPOIOtherGovtID]
				,[RelatedPersonCKYCPOIS01IDNumber]
				,[RelatedPersonCKYCPOIS02IDNumber]
				,[RelatedPersonProofOfIDSubmitted]
				,[SourceSystemSegment]
				,Final.AppRefNumberforImages
				,[HolderforImages]
				,[BranchCode]
				,Final.KYCEmployeeCode AS AddedBy
				,CustomerStatusEffectiveDate
				,CustomerStatus
			FROM #TEMP5 FINAL
			WHERE RN = 1
			) AS FINAL

		INSERT INTO dbo.CKYCDataUploadLog (
			processname
			,methodname
			,activity
			,CreatedOn
			,RecordsProcessed
			,BatchId
			,Response
			)
		VALUES (
			'DF Ckyc Data Upload'
			,'proc_Daily_BYJUPL_CKYCCustomerAndImages'
			,'Data upload'
			,GETDATE()
			,@@ROWCOUNT
			,@BatchId
			,'Data is uploaded in StagingCustom217CustomerFlat table.'
			)

		SELECT OutTab.Cuid
			,OutTab.[ProspectNo]
			,CASE 
				WHEN OutTab.File_Extension = 'pdf'
					THEN CONCAT (
							'PL_' + OutTab.Cuid + '_' + ProspectNo
							,'.pdf'
							)
				ELSE CONCAT (
						'PL_' + OutTab.Cuid + '_' + ProspectNo
						,'.jpg'
						)
				END [NAME]
			,CASE 
				WHEN OutTab.File_Extension = 'pdf'
					THEN '.pdf'
				ELSE '.jpg'
				END Filetype
			,OutTab.File_Extension
			,image_path = STUFF((
					SELECT ';' + InrTab.image_path
					FROM [#TEMP2] InrTab
					WHERE (ParameterName LIKE '%Aadhar%')
						AND InrTab.Cuid = OutTab.Cuid
						AND InrTab.ProspectNo = OutTab.ProspectNo
						AND InrTab.ParametersName = 'Address Proof'
						AND InrTab.File_Extension <> 'xml'
						AND InrTab.File_Extension = OutTab.File_Extension
					ORDER BY InrTab.image_path
						,InrTab.File_Extension
					FOR XML PATH('')
						,TYPE
					).value('.', 'VARCHAR(MAX)'), 1, 1, SPACE(0))
			,ROW_NUMBER() OVER (
				PARTITION BY OutTab.ProspectNo ORDER BY OutTab.File_Extension ASC
				) AS Srno
		INTO #FinalImageList
		FROM [#TEMP2] OutTab
		WHERE (ParameterName LIKE '%Aadhar%')
			AND OutTab.File_Extension <> 'xml'
		GROUP BY OutTab.Cuid
			,OutTab.[ProspectNo]
			,OutTab.File_Extension

		INSERT INTO CustomerImagesDailyPLBYJU (
			TransactionId
			,FileName
			,FilePath
			,Filetype
			,DocumentType
			,AppRefNoForImages
			,Cuid
			,BinaryFormat
			,Product
			,Batchid
			,mkrid
			,AddedOn
			,IsUploaded
			)
		SELECT CONVERT(VARCHAR(100), rn + 10000) AS Transactionid
			,NAME AS FileName
			,Filepath
			,FileType
			,AttachmentCode AS DocumentType
			,[ProspectNo] AS AppRefNoForImages
			,Cuid
			,'N' AS BinaryFormat
			,'PLBYJU' AS Product
			,convert(VARCHAR(100), @BatchId) AS Batchid
			,mkrid
			,getdate() AS AddedOn
			,'N' AS IsUploaded
		FROM (
			SELECT Row_number() OVER (
					ORDER BY tcm.ProspectNo DESC
					) rn
				,tcm.srno AS Transactionid
				,Final.CUID
				,tcm.[ProspectNo]
				,NAME
				,Filepath
				,CASE 
					WHEN CHARINDEX('.', Filepath) > 0
						THEN REVERSE(left(REVERSE(Filepath), charindex('.', REVERSE(Filepath)) - 1))
					ELSE ''
					END AS FileType
				,AttachmentCode
				,tcm.Mkrid
			FROM (
				SELECT cuid
					,[ProspectNo]
					,NAME
					,fileType
					,image_path AS Filepath
					,'aadhar' AS AttachmentCode
				FROM (
					--SELECT  OutTab.Cuid ,OutTab.[ProspectNo] , CONCAT('PL_' +OutTab.Cuid+ '_' + ProspectNo,'.jpg')NAME,
					--'.jpg'Filetype,
					--image_path = STUFF (  ( SELECT ';'+InrTab.image_path
					--FROM [#TEMP2] InrTab
					--WHERE  ( ParameterName like '%Aadhar%' ) and InrTab.Cuid = OutTab.Cuid
					--AND InrTab.ProspectNo = OutTab.ProspectNo			
					--AND InrTab.ParametersName = 'Address Proof'
					--ORDER BY InrTab.image_path
					--FOR XML PATH(''),TYPE).value('.','VARCHAR(MAX)'), 1,1,SPACE(0))
					--FROM [#TEMP2] OutTab 
					--where ( ParameterName like '%Aadhar%')
					--GROUP BY OutTab.Cuid,OutTab.[ProspectNo]
					SELECT Cuid
						,ProspectNo
						,NAME
						,Filetype
						,image_path
					FROM #FinalImageList
					WHERE Srno = 1
					) AS test
				
				UNION ALL
				
				SELECT Cuid
					,ProspectNo
					,CASE 
						WHEN ParametersName = 'photograph'
							THEN 'PH_' + Cuid + '_' + ProspectNo + '.jpg'
						ELSE CASE 
								WHEN CHARINDEX('\', image_path) > 0
									THEN REVERSE(LEFT(REVERSE(image_path), CHARINDEX('\', REVERSE(image_path)) - 1))
								ELSE ''
								END
						END AS NAME
					,CASE 
						WHEN CHARINDEX('.', Image_Path) > 0
							THEN REVERSE(LEFT(reverse(Image_Path), CHARINDEX('.', REVERSE(Image_Path)) - 1))
						ELSE ''
						END AS FileType
					,Image_Path AS Filepath
					,CASE 
						WHEN ParametersName = 'photograph'
							THEN 'photograph'
						ELSE lower(ParameterName)
						END AttachmentCode
				FROM [#TEMP2] InrTab
				WHERE ParameterName NOT LIKE '%Aadhar%'
				) AS Final
			INNER JOIN SME..TBL_CLIENTMASTER TCM WITH (NOLOCK) ON FINAL.[PROSPECTNO] = TCM.PROSPECTNO
			) AS t
		WHERE FileType != ''

		INSERT INTO #Log
		VALUES (
			'Image records for CERSAI'
			,@@ROWCOUNT
			)

		INSERT INTO dbo.CKYCDataUploadLog (
			processname
			,methodname
			,activity
			,CreatedOn
			,RecordsProcessed
			,BatchId
			,Response
			)
		VALUES (
			'DF Ckyc Data Upload'
			,'proc_Daily_BYJUPL_CKYCCustomerAndImages'
			,'Image Data upload'
			,GETDATE()
			,@@ROWCOUNT
			,@BatchId
			,'Image data is uploaded in CustomerImagesDailyPLBYJU table.'
			)

		SELECT CONVERT(VARCHAR(100), a.Srno) AS ID
			,FileName NAME
			,FilePath
			,lower(Filetype) AS Filetype
			,DocumentType
			,a.AppRefNoForImages
			,a.Cuid CUID
			,mkrid
			,'N' BinaryFormat
			,NULL AS ClientImage
			,Batchid
			,Product
		FROM CustomerImagesDailyPLBYJU a WITH (NOLOCK)
		--left join ( select distinct AppRefnumberForImages,SourceSysytemCustomerCode
		--		 from StagingCKYCAttachmentView  with(Nolock)
		--		 where Product ='PLBYJU' and  AttachmentCode!='photograph' ) b on a.Cuid =b.SourceSysytemCustomerCode    
		WHERE a.Product = 'PLBYJU'
			AND a.IsUploaded = 'N'
			AND batchId = @BatchId

		INSERT INTO dbo.CKYCDataUploadLog (
			processname
			,methodname
			,activity
			,CreatedOn
			,RecordsProcessed
			,BatchId
			,Response
			)
		VALUES (
			'DF Ckyc Data Upload'
			,'proc_Daily_BYJUPL_CKYCCustomerAndImages'
			,'Process-End'
			,GETDATE()
			,0
			,@BatchId
			,'Process-End'
			)
	END TRY

	BEGIN CATCH
		DECLARE @ID VARCHAR(15) = CAST(@BatchId AS VARCHAR)

		EXECUTE proc_SaveDBError @ID
	END CATCH
END 
