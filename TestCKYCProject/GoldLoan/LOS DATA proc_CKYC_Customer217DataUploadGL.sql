--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
TEXT

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---=====================================================================
-- CREATED BY : Ravindra Sapkal
-- CREATED ON : 30 Oct 2020
-- Reason     : CKCY Customer upload
--Modifid By : Ravindra Sapkal
--Modifid on/Reason:  Added for the already uploaded on Trackwizz check om 28 April 2021
--Modifiedon : Added logical changes for the permanent address proof
--Modifiedon : Added logical changes for the Correspondence address proof
---=====================================================================
CREATE PROC [dbo].[proc_CKYC_Customer217DataUploadGL]
AS
BEGIN
	DECLARE @CurrentDate VARCHAR(8) = CONVERT(VARCHAR(8), GETDATE(), 112)
	--------Generate Batch Id 
	DECLARE @batchId INT = 0

	CREATE TABLE #temp (
		STATUS VARCHAR(10)
		,BatchId INT
		);

	INSERT INTO #temp (
		[STATUS]
		,BatchId
		)
	EXECUTE InsertUpdateBatchDetails 'GL'
		,0
		,'GL_Job'
		,0
		,0
		,0;

	SELECT @BatchId = BatchId
	FROM #temp
	WHERE STATUS = 0;

	---------------------------------------
	-------------------End here-------------------------------------
	--Added for the already uploaded check om 28 April 2021
	SELECT DISTINCT CUID COLLATE SQL_Latin1_General_CP1_CI_AS AS CUID
	INTO #UploadedData
	FROM (
		SELECT FromSourceSystemCustomerCode AS CUID
		FROM [FRANKLIN.iifl.in].TrackWizz.dbo.CKYCOUTWARDVIEW WITH (NOLOCK)
		WHERE FromSourceSystem = 'GoldLoan'
		
		UNION
		
		SELECT cuid collate Latin1_General_CI_AI AS CUID
		FROM [dbo].[GL_CKYCIdGenerated] WITH (NOLOCK)
		
		UNION
		
		SELECT DISTINCT SourceSystemCustomerCode AS CUID
		FROM [FRANKLIN.iifl.in].TrackWizz.dbo.[CoreCRMCustomerHistory] WITH (NOLOCK)
		WHERE SourceSystem = 'GoldLoan'
			AND RejectionCodes IS NULL
		) AS A;

	WITH [CTE]
	AS (
		SELECT [CM].ProspectNo AS [ProspectNo]
			,[Sex]
			,[Title]
			,[FName]
			,[MName]
			,[LName]
			,[CM].[DOB]
			,[IsMaried]
			,[FathersName]
			,[Education]
			,[EmploymentType]
			,[NetAnnualIncome]
			,[NoOfYrs_Emp]
			,[Spouce_Earning]
			,[LoanPurpose]
			,[Dependents]
			,[Property_Value]
			,[TimeToCall]
			,[PreferredModeforComm]
			,[Institution]
			,[Portfolio]
			,[Location]
			,[SubLocation]
			,[SchemeType]
			,[ProductType]
			,[Scheme]
			,[RateOfInterest]
			,[LoanAmt]
			,[Tenure]
			,[Add1]
			,[Add2]
			,[Add3]
			,[PinCode]
			,[ContatcNo]
			,[Mob]
			,[Fax]
			,[Email]
			,[Country]
			,[CM].[State]
			,[CM].[City]
			,[Ownership]
			,[StayedMonths]
			,[VoterID]
			,[PANNo]
			,[PassportNo]
			,[DrivingLicense]
			,[SalesManager]
			,[SpouseName]
			,[MotherName]
			,[IsPrioritySector]
			,[ClientType]
			,[cinfrn]
			,[tanno]
			,[contactFname]
			,[contactLname]
			,[contactFathername]
			,[contactPAN]
			,[CompanyType]
			,[ClientStatus]
			,[ClientPayType]
			,[CM].[Mkrdt]
			,[CM].[Mkrid]
			,CM.[CreatedTime]
			,[cm_closuredate]
			,[cm_closurereason]
			,[scheme_gold]
			,[CM].[CUID]
			,[Risk]
			,[Closure_mkrid]
			,[SourceofIncome]
			,[Networth]
			,[Scheme_Gold_Persent]
			,[LandHolding]
			,[Category]
			,[IsWeb]
			,[FAMILYID]
			,CASE 
				WHEN LEN(REPLACE([CM].[AdharCardNo], ' ', '')) > 12
					THEN ''
				ELSE REPLACE([CM].[AdharCardNo], ' ', '')
				END [AdharCardNo]
			,[ROIDiff]
			,[CM].[PassportExpiryDate]
			,[CM].[DrivingLicenseExpiryDate]
			,[MaidenName]
			,[Mobile2]
			,[Contact2]
			,[STD]
			,[Form60]
			,[FatherMiddleName]
			,[FatherLastName]
			,[MotherMiddleName]
			,[MotherLastName]
			,[SpouseMiddleName]
			,[SpouseLastName]
			,[MaidenMiddleName]
			,[MaidenLastName]
			,[SpousePANNO]
			,[PrefferedLanguageCode]
			,[Emp].[EmpName] AS [Maker Name]
			,[SutraCode]
			,[Emp].[Designation]
			,[SubLocation_Description] AS [BranchName]
			,[U].[cuid] AS [UCID]
			,[CT].[City_Description]
			,[CA].[ad_Add1]
			,[CA].[ad_Add2]
			,[CA].[ad_Add3]
			,[CA].[ad_PinCode]
			,[CTA].[City_Description] AS [ad_city]
			,CASE 
				WHEN ISNULL([AdharCardNo], '') <> ''
					THEN 'AadharCard'
				WHEN ISNULL([PassportNo], '') <> ''
					THEN 'Passport'
				WHEN ISNULL([VoterID], '') <> ''
					THEN 'VoterID'
				WHEN ISNULL([DrivingLicense], '') <> ''
					THEN 'DrivingLicence'
				WHEN ISNULL([PANNo], '') <> ''
					THEN 'PAN'
				ELSE 'AadharCard'
				END IDProof
			,CASE 
				WHEN ISNULL([AdharCardNo], '') <> ''
					THEN 'AadharCard'
				WHEN ISNULL([PassportNo], '') <> ''
					THEN 'Passport'
				WHEN ISNULL([VoterID], '') <> ''
					THEN 'VoterID'
				WHEN ISNULL([DrivingLicense], '') <> ''
					THEN 'DrivingLicence'
				ELSE 'AadharCard'
				END AS [PermanentAddressProof]
			,ROW_NUMBER() OVER (
				PARTITION BY [CM].[CUID] ORDER BY [CM].[Srno] DESC
				) AS [rn]
			,(
				CASE 
					WHEN ISNULL([CM].CUID, '') <> ''
						AND ISNULL([CM].ProspectNo, '') <> ''
						THEN 'Y'
					ELSE 'N'
					END
				) IsValid
			,'N' AS IsImageUploaded
			,CM.Mkrid AS AddedBy
			,[CM].clientStatus AS CustomerStatus
			,[CM].Mkrdt AS CustomerStatusEffectiveDate
		FROM IIFLGOLD.dbo.[tbl_clientmaster] AS [CM] WITH (NOLOCK)
		LEFT OUTER JOIN IIFLGOLD.[dbo].[tbl_clientaddresses] AS [CA] WITH (NOLOCK) ON [CM].[ProspectNo] = [CA].[ad_ProspectNo]
			AND [CA].[ad_AddressType] <> 'CURRENT ADDRESS AS PER KYC'
		LEFT OUTER JOIN [deathrace.indiainfoline.in].IWIN.dbo.Vw_EmployeeDataAllWithLeft AS [Emp] WITH (NOLOCK) ON [CM].[Mkrid] = [Emp].[EmpId] --- all employee
			--LEFT OUTER JOIN iwin.dbo.Vw_EmployeeDataAllWithLeft AS [Emp] ON [CM].[Mkrid] = [Emp].[EmpId] --- all employee
		LEFT OUTER JOIN IIFLGOLD.[dbo].[City_Master] AS [CT] WITH (NOLOCK) ON [CM].[City] = [CT].[City_Code]
		LEFT OUTER JOIN IIFLGOLD.[dbo].[City_Master] AS [CTA] WITH (NOLOCK) ON [CA].[ad_City] = [CTA].[City_Code]
		LEFT OUTER JOIN IIFLGOLD.[dbo].[sublocation_master] AS [LOC] WITH (NOLOCK) ON [CM].[SubLocation] = [LOC].[SubLocation_Code]
		--LEFT OUTER JOIN [BREAKINGUP.IIFL.IN].[UCID].[dbo].[Unique_Customers] AS [U] ON [CM].[CUID] = [U].[CustomerID] AND [U].[Business] = 'GoldLoan'
		LEFT OUTER JOIN [UCID].[dbo].[Unique_Customers] AS [U] WITH (NOLOCK) ON [CM].[CUID] = [U].[CustomerID]
			AND (
				[U].[Business] = 'GoldLoan'
				OR [U].[Business] = 'Gold Loan'
				)
		WHERE CONVERT(VARCHAR(8), [CM].Mkrdt, 112) = @CurrentDate
			AND [CM].portfolio IN (
				'66'
				,'101'
				,'103'
				,'104'
				)
		)
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
	SELECT 'IIFL12' AS ParentCompany
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
			ELSE 'Suspended'
			END AS CustomerStatus
		,CustomerStatusEffectiveDate
		,'' RelatedPartyStatus
		,'' RelatedPartyStatusEffectiveDate
		,[ConstitutionType] AS CustomerType
		,'' CustomerSubType
		,LTRIM(RTRIM(REPLACE(ISNULL(Prefix, ''), '.', ''))) AS Prefix
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
		,REPLACE(KYCPlaceOfDeclaration, '-', ' ') AS [KYCPlaceOfDeclaration]
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
		,[PermanentAddressProof] AS CorrespondenceAddressProof
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
		,CASE 
			WHEN ISNULL(PassportNumber, '') != ''
				THEN 'IN'
			ELSE ''
			END AS PassportIssueCountry
		,CASE 
			WHEN ISNULL(PassportNumber, '') = ''
				THEN (
						SELECT TOP 1 ISNULL(b.PassportNo, '')
						FROM IIFLGOLD.dbo.tbl_clientmaster b WITH (NOLOCK)
						WHERE b.CUID = Final.SourcesystemCustCode
							AND ISNULL(b.PassportNo, '') != ''
						)
			ELSE PassportNumber
			END PassportNumber
		,
		--,[PassportExpiryDate]
		CASE 
			WHEN ISNULL(PassportNumber, '') = ''
				THEN (
						SELECT TOP 1 ISNULL(PassportExpiryDate, '')
						FROM IIFLGOLD.dbo.tbl_clientmaster b WITH (NOLOCK)
						WHERE b.CUID = Final.SourcesystemCustCode
							AND ISDATE([PassportExpiryDate]) = 1
						)
			ELSE PassportExpiryDate
			END PassportExpiryDate
		,CASE 
			WHEN ISNULL(VoterIdCard, '') = ''
				THEN (
						SELECT TOP 1 ISNULL(VoterID, '')
						FROM IIFLGOLD.dbo.tbl_clientmaster b WITH (NOLOCK)
						WHERE b.CUID = Final.SourcesystemCustCode
							AND ISNULL(b.VoterID, '') != ''
						)
			ELSE ISNULL(VoterIdCard, '')
			END AS VoterIdCard
		,CASE 
			WHEN ISNULL([PAN], '') = ''
				THEN (
						SELECT TOP 1 ISNULL(PANNo, '')
						FROM IIFLGOLD.dbo.tbl_clientmaster b WITH (NOLOCK)
						WHERE b.CUID = Final.SourcesystemCustCode
							AND ISNULL(PANNo, '') != ''
						)
			ELSE ISNULL(PAN, '')
			END AS PAN
		,
		--,[DrivingLicenseNumber]
		CASE 
			WHEN ISNULL(DrivingLicenseNumber, '') = ''
				THEN (
						SELECT TOP 1 ISNULL(DrivingLicense, '')
						FROM IIFLGOLD.dbo.tbl_clientmaster b WITH (NOLOCK)
						WHERE b.CUID = Final.SourcesystemCustCode
							AND ISNULL(b.DrivingLicense, '') != ''
						)
			ELSE DrivingLicenseNumber
			END AS DrivingLicenseNumber
		,CASE 
			WHEN ISNULL(DrivingLicenseExpiryDate, '') = ''
				THEN (
						SELECT TOP 1 ISNULL(DrivingLicenseExpiryDate, '')
						FROM IIFLGOLD.dbo.tbl_clientmaster b WITH (NOLOCK)
						WHERE b.CUID = Final.SourcesystemCustCode
							AND ISDATE(b.DrivingLicenseExpiryDate) = 1
						)
			ELSE DrivingLicenseExpiryDate
			END AS DrivingLicenseExpiryDate
		,CASE 
			WHEN ISNULL(Aadhaar, '') = ''
				THEN (
						SELECT TOP 1 ISNULL(b.AdharCardNo, '')
						FROM IIFLGOLD.dbo.tbl_clientmaster b WITH (NOLOCK)
						WHERE b.CUID = Final.SourcesystemCustCode
							AND ISNULL(b.AdharCardNo, '') != ''
						)
			ELSE ISNULL(Aadhaar, '')
			END AS [Aadhaar]
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
		,AddedBy
		,GETDATE() AS AddedOn
		,'' AS GUID
		,Form60 AS FORMSixty
		--,CASE WHEN ISNULL([PANNo], '') != '' THEN NULL 
		--               WHEN ISNULL((SELECT TOP 1
		--                          ISNULL(PANNo, '')
		--                   FROM IIFLGOLD.dbo.tbl_clientmaster b WITH(NOLOCK)
		--                   WHERE b.CUID = Final.cuid AND ISNULL(PANNo, '') != ''),'') != '' THEN NULL
		--	   ELSE 1 END AS FORMSixty
		,'' AS NPRLetter
		,'01' AS KYCAttestationType
		,@batchId AS batchId
		,'Y' AS IsValid
	FROM (
		SELECT 'GL' + CONVERT(VARCHAR(10), GETDATE(), 112) + CONVERT(VARCHAR(2), DATEPART(HOUR, GETDATE())) + CONVERT(VARCHAR(2), DATEPART(MINUTE, GETDATE())) + RIGHT('0000000' + CONVERT(VARCHAR(10), DENSE_RANK() OVER (
						ORDER BY [CTE].[CUID]
						)), 10) AS [TransactionID]
			,'GoldLoan' AS [SourceSystemName]
			,[CTE].[CUID] AS [SourcesystemCustCode]
			,'No' AS [SmallCustomer]
			,'No' AS [EkycOTPbased]
			,'New' AS [TransactionType]
			,CASE 
				WHEN ISDATE([CTE].[CreatedTime]) = 1
					THEN FORMAT([CTE].[CreatedTime], 'dd-MMM-yyyy')
				ELSE NULL
				END AS [SourceSystemCustomerCreationDate]
			,'1' AS [ConstitutionType]
			,[CTE].[Title] AS [Prefix]
			,dbo.RemoveNonAlphaCharacters([CTE].[FName]) AS [FirstName]
			,'' [MiddleName]
			,
			--[CTE].[MName] AS [MiddleName], 
			'' [LastName]
			,
			--  [CTE].[LName] AS [LastName],
			'' [MaidenPrefix]
			,
			--  CASE WHEN ISNULL([CTE].[MaidenName], '') <> '' THEN 'Mrs' ELSE '' END AS [MaidenPrefix], 
			'' [MaidenFirstName]
			,
			--[CTE].[MaidenName] AS [MaidenFirstName], 
			'' [MaidenMiddleName]
			,
			---[CTE].[MaidenMiddleName] AS [MaidenMiddleName], 
			'' [MaidenLastName]
			,
			-- [CTE].[MaidenLastName] AS [MaidenLastName], 
			CASE 
				WHEN dbo.RemoveNonAlphaCharacters(ISNULL([CTE].[FathersName], '')) <> ''
					THEN 'Mr'
				ELSE ''
				END AS [FatherPrefix]
			,REPLACE(dbo.RemoveNonAlphaCharacters([CTE].[FathersName]), '.', ' ') AS [FatherFirstName]
			,'' [FatherMiddleName]
			,
			--[CTE].[FatherMiddleName] AS [FatherMiddleName], 
			'' [FatherLastName]
			,
			--[CTE].[FatherLastName] AS [FatherLastName], 
			---Spouse
			CASE 
				WHEN dbo.RemoveNonAlphaCharacters(ISNULL([CTE].[SpouseName], '')) <> ''
					AND dbo.RemoveNonAlphaCharacters(ISNULL([CTE].[FathersName], '')) = ''
					THEN (
							CASE 
								WHEN [CTE].[Sex] = 'M'
									THEN 'Mrs'
								ELSE 'Mr'
								END
							)
				ELSE ''
				END AS [SpousePrefix]
			,CASE 
				WHEN dbo.RemoveNonAlphaCharacters(ISNULL([CTE].[FathersName], '')) = ''
					THEN REPLACE(ISNULL(dbo.RemoveNonAlphaCharacters([CTE].[SpouseName]), ''), '.', ' ')
				END AS [SpouseFirstName]
			,'' [SpouseMiddleName]
			,
			--[CTE].[SpouseMiddleName] AS [SpouseMiddleName], 
			'' [SpouseLastName]
			,
			--[CTE].[SpouseLastName] AS [SpouseLastName], 
			CASE 
				WHEN dbo.RemoveNonAlphaCharacters(ISNULL([CTE].[MotherName], '')) <> ''
					THEN 'Mrs'
				ELSE ''
				END AS [MotherPrefix]
			,REPLACE(dbo.RemoveNonAlphaCharacters([CTE].[MotherName]), '.', ' ') AS [MotherFirstName]
			,'' [MotherMiddleName]
			,
			--[CTE].[MotherMiddleName] AS [MotherMiddleName], 
			'' [MotherLastName]
			,
			--[CTE].[MotherLastName] AS [MotherLastName], 
			[CTE].[Sex] AS [Gender]
			,CASE 
				WHEN [CTE].[IsMaried] = 'Married'
					THEN 'M'
				WHEN [CTE].[IsMaried] = 'UnMarried'
					THEN 'U'
				ELSE 'O'
				END AS [MaritalStatus]
			,'IN' AS [Citizenship]
			,CASE 
				WHEN [CTE].[SourceofIncome] = 'Agriculture'
					THEN 'X-01'
				WHEN [CTE].[SourceofIncome] = 'Others'
					THEN 'X-01'
				WHEN [CTE].[SourceofIncome] = 'Business'
					THEN 'B-01'
				WHEN [CTE].[SourceofIncome] = 'Salaried'
					THEN 'S-02'
				ELSE 'X-01'
				END AS [OccupationType]
			,CASE 
				WHEN ISDATE([CTE].[DOB]) = 1
					THEN FORMAT(CONVERT(DATE, [CTE].[DOB]), 'dd-MMM-yyyy')
				ELSE NULL
				END AS [DateofBirth]
			,'01' AS [ResidentialStatus]
			,'' [EmailId]
			,
			--[CTE].[Email] AS [EmailId], 
			CASE 
				WHEN ISDATE([CTE].[CreatedTime]) = 1
					THEN FORMAT([CTE].[CreatedTime], 'dd-MMM-yyyy')
				ELSE NULL
				END AS [KYCDateOfDeclaration]
			,[CTE].[BranchName] AS [KYCPlaceOfDeclaration]
			,CASE 
				WHEN ISDATE([CTE].[CreatedTime]) = 1
					THEN FORMAT([CTE].[CreatedTime], 'dd-MMM-yyyy')
				ELSE NULL
				END AS [KYCVerificationDate]
			,ISNULL([CTE].[Maker Name], '') AS [KYCEmployeeName]
			,ISNULL([Designation], '') AS [KYCEmployeeDesignation]
			,'HO' AS [KYCVerificationBranch]
			,[CTE].[Mkrid] AS [KYCEmployeeCode]
			,'01' AS [PermanentCKYCAddType]
			,'IN' AS [PermanentCountry]
			,[CTE].[PinCode] AS [PermanentPin]
			,dbo.RemoveInvalidCharactersFromAddressFields([CTE].[Add1]) AS [PermanentAddressLine1]
			,
			--- replace special character with space
			dbo.RemoveInvalidCharactersFromAddressFields([CTE].[Add2]) AS [PermanentAddressLine2]
			,
			--- replace special character with space
			dbo.RemoveInvalidCharactersFromAddressFields([CTE].[Add3]) AS [PermanentAddressLine3]
			,
			--- replace special character with space
			'' AS [PermanentDistrict]
			,ISNULL([CTE].[City_Description], '') AS [PermanentCity]
			,'' AS [PermanentState]
			,[CTE].[PermanentAddressProof]
			,'IN' AS [CorrespondenceGlobalCountry]
			,[CTE].[ad_PinCode] AS [CorrespondenceGlobalPin]
			,
			--dbo.RemoveInvalidCharactersFromAddressFields([CTE].[ad_Add1]) AS [CorrespondenceGlobalAddressLine1],
			dbo.RemoveInvalidCharactersFromAddressFields([CTE].[Add1]) AS [CorrespondenceGlobalAddressLine1]
			,
			--- replace special character with space
			--dbo.RemoveInvalidCharactersFromAddressFields([CTE].[ad_Add2]) AS [CorrespondenceGlobalAddressLine2],
			dbo.RemoveInvalidCharactersFromAddressFields([CTE].[Add2]) AS [CorrespondenceGlobalAddressLine2]
			,
			--- replace special character with space
			--dbo.RemoveInvalidCharactersFromAddressFields([CTE].[ad_Add3]) AS [CorrespondenceGlobalAddressLine3],
			dbo.RemoveInvalidCharactersFromAddressFields([CTE].[Add3]) AS [CorrespondenceGlobalAddressLine3]
			,
			--- replace special character with space
			'' AS [CorrespondenceGlobalDistrict]
			,ISNULL([CTE].[ad_city], '') AS [CorrespondenceGlobalCity]
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
			,'' AS [TaxResidencyCountry]
			,NULL AS [ResidentialSTDCode]
			,NULL AS [ResidentialTelephoneNumber]
			,NULL AS [OfficeSTDCode]
			,NULL AS [OfficeTelephoneNumber]
			,NULL AS [MobileISD]
			,CASE 
				WHEN ISNUMERIC(LEFT([CTE].[Mob], 10)) = 1
					THEN LEFT([CTE].[Mob], 10)
				ELSE NULL
				END AS [MobileNumber]
			,NULL AS [FaxSTD]
			,NULL AS [FaxNumber]
			,NULL AS [CKYCID]
			,
			--,CASE 
			--	WHEN [CTE].[PermanentAddressProof] = 'Passport'
			--		THEN [CTE].[PassportNo]
			--	ELSE ''
			--	END AS [PassportNumber] --commented by ravindra on 8 mar 2019
			[CTE].[PassportNo] AS [PassportNumber]
			,
			--,CASE 
			--	WHEN [CTE].[PermanentAddressProof] = 'Passport'
			--		AND ISDATE([CTE].[PassportExpiryDate]) = 1
			--		THEN FORMAT(CONVERT(DATE, [CTE].[PassportExpiryDate]), 'dd-MMM-yyyy')
			--	ELSE NULL
			--	END AS [PassportExpiryDate] --commented by ravindra on 8 mar 2019
			CASE 
				WHEN ISDATE([CTE].[PassportExpiryDate]) = 1
					THEN FORMAT(CONVERT(DATE, [CTE].[PassportExpiryDate]), 'dd-MMM-yyyy')
				ELSE NULL
				END AS [PassportExpiryDate]
			,
			--,CASE 
			--	WHEN [CTE].[PermanentAddressProof] = 'VoterID'
			--		THEN [CTE].[VoterID]
			--	ELSE ''
			--	END AS [VoterIdCard]
			[CTE].[VoterID] AS [VoterIdCard]
			,
			--,CASE 
			--	WHEN [CTE].[IDProof] = 'PAN'
			--		OR (
			--			dbo.RemoveNonAlphaCharacters(ISNULL([CTE].[FathersName], '')) = ''
			--			AND REPLACE(dbo.RemoveNonAlphaCharacters([CTE].[SpouseName]), '.', ' ') <> ''
			--			)
			--		THEN [CTE].[PANNo]
			--	ELSE ''
			--	END AS [PAN] --commented by ravindra on 8 mar 2019
			ISNULL([CTE].[PANNo], '') AS [PAN]
			,
			--,CASE 
			--	WHEN [CTE].[PermanentAddressProof] = 'DrivingLicence'
			--		THEN [CTE].[DrivingLicense]
			--	ELSE ''
			--	END AS [DrivingLicenseNumber] --commented by ravindra on 8 mar 2019
			[CTE].[DrivingLicense] AS [DrivingLicenseNumber]
			,
			--,CASE 
			--	WHEN [CTE].[PermanentAddressProof] = 'DrivingLicence'
			--		AND ISDATE([CTE].[DrivingLicenseExpiryDate]) = 1
			--		THEN FORMAT(CONVERT(DATE, [CTE].[DrivingLicenseExpiryDate]), 'dd-MMM-yyyy')
			--	ELSE NULL
			--	END AS [DrivingLicenseExpiryDate] --commented by ravindra on 8 mar 2019
			CASE 
				WHEN ISDATE([CTE].[DrivingLicenseExpiryDate]) = 1
					THEN FORMAT(CONVERT(DATE, [CTE].[DrivingLicenseExpiryDate]), 'dd-MMM-yyyy')
				ELSE NULL
				END AS [DrivingLicenseExpiryDate]
			,CASE 
				WHEN ISNUMERIC(LEFT([CTE].[AdharCardNo], 12)) = 1
					THEN [CTE].[AdharCardNo]
				ELSE NULL
				END AS [Aadhaar]
			,'' AS [NREGA]
			,'' AS [CKYCPOIOtherCentralGovtID]
			,'' AS [CKYCPOIS01IDNumber]
			,'' AS [CKYCPOIS02IDNumber]
			,[CTE].[IDProof] AS [ProofOfIDSubmitted]
			,NULL AS [CustomerDemiseDate]
			,0 AS [Minor]
			,'' AS [SourcesystemRelatedPartyode]
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
			,[CTE].[SutraCode] BranchCode
			,CTE.IsValid
			,CTE.IsImageUploaded
			,CTE.AddedBy
			,CTE.Form60
			,CTE.CustomerStatus
			,FORMAT(CONVERT(DATE, [CTE].CustomerStatusEffectiveDate), 'dd-MMM-yyyy') AS CustomerStatusEffectiveDate
		FROM [CTE]
		WHERE rn = 1
			AND CUID IS NOT NULL
		) AS Final;

	DELETE
	FROM Clients.dbo.StagingCustom217CustomerFlat
	WHERE BatchId = @batchId
		AND SourceSystemName = 'Goldloan'
		AND SourceSystemCustomerCode IN (
			SELECT DISTINCT cuid
			FROM #UploadedData
			)

	SELECT SourceSystemCustomerCode
		,ApplicationRefNumber
		,IsValid
	INTO #UploadedCustomer
	FROM Clients.dbo.StagingCustom217CustomerFlat W WITH (NOLOCK)
	WHERE BatchId = @batchId
		AND IsValid = 'Y'
		---------------Start for image processing 
		;

	WITH CTE_POAandPOI
	AS (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY tkd.CUID
				,tkd.FamilyID ORDER BY tkd.Srno DESC
				) AS rn
			,tkd.Srno AS TransactionId
			,tkd.image_path AS FilePath
			,tkd.mkrid
			,tkd.DocumentID
			,tc.ApplicationRefNumber AS ProspectNo
			,tkd.CUID
			,tkd.ImageName AS FileName
			,tkd.FamilyID
		FROM IIFLGOLD.dbo.tbl_KYCdoc_Details tkd WITH (NOLOCK)
		INNER JOIN #UploadedCustomer tc ON tc.SourceSystemCustomerCode = tkd.CUID
		INNER JOIN IIFLGOLD.dbo.tbl_clientmaster tca WITH (NOLOCK) ON tkd.CUID = tca.CUID
		WHERE tc.IsValid = 'Y'
			AND image_path IS NOT NULL
			AND DocumentID > 0
			AND (
				(
					tkd.FamilyID IN (
						7
						,8
						)
					AND tkd.IsKYCDoc = 'Y'
					)
				OR (tkd.FamilyID = 17)
				)
			--AND tca.Mkrdt  > CONVERT (DATETIME, @CurrentDate)
		)
		,CTE_Image
	AS (
		SELECT TransactionId AS ID
			,FilePath
			,CASE 
				WHEN FilePath LIKE '%\%'
					THEN RIGHT([FilePath], CHARINDEX('\', REVERSE([FilePath]), - 1) - 1)
				ELSE ''
				END AS FileName
			,CASE 
				WHEN FamilyID = 7
					AND tdm.Srno = 17
					THEN 'passport - address proof' --and FamilyDescription ='Passport' 
				WHEN FamilyID = 7
					AND tdm.Srno = 53
					THEN 'aadharcard - address proof' -- and FamilyDescription ='Aadhaar Card'
				WHEN FamilyID = 7
					AND tdm.Srno = 54
					THEN 'drivinglicense - address proof' --and FamilyDescription ='Driving License'
				WHEN FamilyID = 7
					AND tdm.Srno = 55
					THEN 'votercard - address proof' --and FamilyDescription ='Election ID Card'
				WHEN FamilyID = 8
					AND tdm.Srno = 56
					AND FamilyDescription = 'Aadhaar Card'
					THEN 'aadharcard - id proof'
				WHEN FamilyID = 8
					AND tdm.Srno = 22
					AND FamilyDescription = 'Passport'
					THEN 'valid passport- id proof'
				WHEN FamilyID = 8
					AND tdm.Srno = 23
					AND FamilyDescription = 'Driving License'
					THEN 'drivinglicense - id proof'
				WHEN FamilyID = 8
					AND tdm.Srno = 24
					AND FamilyDescription = 'Election ID Card'
					THEN 'votercard - id proof'
				WHEN FamilyID = 8
					AND tdm.Srno = 25
					AND FamilyDescription = 'PAN Card'
					THEN 'pancard - id proof'
				WHEN FamilyID = 17
					AND FamilyDescription = 'Electricity Bill'
					OR FamilyDescription = 'Telephone Bill'
					OR FamilyDescription = 'Gas Bill'
					OR FamilyDescription = 'Water Bill'
					THEN 'Utility Bills'
				WHEN FamilyID = 17
					THEN FamilyDescription
				ELSE ''
				END AS AttachmentCode
			,CUID
			,ProspectNo
			,CTE_POAandPOI.mkrid
			,FamilyID
		FROM CTE_POAandPOI
		INNER JOIN IIFLGOLD.dbo.Tbl_DocumentMaster tdm WITH (NOLOCK) ON tdm.Srno = CTE_POAandPOI.DocumentID
		WHERE rn = 1
		)
	SELECT ROW_NUMBER() OVER (
			PARTITION BY CUID ORDER BY ID DESC
			) rn
		,ID
		,FileName
		,FilePath
		,CASE 
			WHEN FilePath LIKE '%.%'
				THEN REVERSE(SUBSTRING(REVERSE(FilePath), 1, CHARINDEX('.', REVERSE(FilePath)) - 1))
			END FileType
		,ProspectNo
		,CUID
		,AttachmentCode
		,mkrid
		,FamilyID
	INTO #CustomerImages
	FROM CTE_Image
	WHERE AttachmentCode != '';

	SELECT CUID
		,AttachmentCode
		,ProspectNo
	INTO #tempProofAddress
	FROM (
		SELECT ROW_NUMBER() OVER (
				PARTITION BY CUID ORDER BY ProspectNo
				) AS rn
			,CASE 
				WHEN AttachmentCode LIKE '%aadharcard%'
					THEN 'AadharCard'
				WHEN AttachmentCode LIKE '%pan%'
					THEN 'PAN'
				WHEN AttachmentCode LIKE '%voter%'
					THEN 'VoterID'
				WHEN AttachmentCode LIKE '%passport%'
					THEN 'Passport'
				WHEN AttachmentCode LIKE '%driving%'
					THEN 'DrivingLicence'
				ELSE ISNULL(AttachmentCode, '')
				END AttachmentCode
			,CUID
			,ProspectNo
		FROM #CustomerImages a
		WHERE FamilyID IN (
				7
				,8
				)
		) AS final
	WHERE rn = 1;

	---Update permanent address proof 
	UPDATE CUS
	SET CUS.PermanentAddressProof = addr.AttachmentCode
		,ProofOfIDSubmitted = addr.AttachmentCode
		,CUS.CorrespondenceAddressProof = addr.AttachmentCode
	FROM Clients.dbo.StagingCustom217CustomerFlat Cus
	INNER JOIN #tempProofAddress addr ON Cus.SourceSystemCustomerCode = addr.CUID
		AND Cus.ApplicationRefNumber = addr.ProspectNo
		AND ISNULL(addr.AttachmentCode, '') != 'PAN'
	WHERE BatchId = @BatchId
		AND Cus.SourceSystemName = 'Goldloan'
		AND IsValid = 'Y'
		AND ISNULL(addr.AttachmentCode, '') != '';

	---END here 
	SELECT StagingCustom217CustomerFlatId
		,SourceSystemCustomerCode
		,ApplicationRefNumber
		,InvalidColRemarks
	INTO #tempInValidRecord
	FROM (
		SELECT StagingCustom217CustomerFlatId
			,SourceSystemCustomerCode
			,ApplicationRefNumber
			,(
				CASE 
					WHEN (
							ISNULL(PermanentAddressProof, '') = ''
							OR ISNULL(PermanentAddressProof, '') LIKE '%PAN%'
							)
						THEN 'Address proof is required.'
					WHEN PermanentAddressProof LIKE '%Aadhar%'
						AND ISNULL(Aadhaar, '') = ''
						THEN 'Aadhar number is for uploaded image.'
					WHEN PermanentAddressProof LIKE '%PAN%'
						THEN 'Pan is invalid address proof'
					WHEN PermanentAddressProof LIKE '%Passport%'
						AND ISNULL(PassportNumber, '') = ''
						THEN 'Passport number not provided for uploaded image'
					WHEN PermanentAddressProof LIKE '%VoterID%'
						AND ISNULL(VoterIdCard, '') = ''
						THEN 'Voter number not provided for uploaded image'
					WHEN PermanentAddressProof LIKE '%Driving%'
						AND ISNULL(DrivingLicenseNumber, '') = ''
						THEN 'Driving Licence number not provided for uploaded image'
					ELSE ''
					END
				) AS InvalidColRemarks
		FROM Clients.dbo.StagingCustom217CustomerFlat WITH (NOLOCK)
		WHERE batchId = @BatchId
			AND SourceSystemName = 'Goldloan'
			AND CustomerIntegrationStatus IS NULL
			AND IsValid = 'Y'
			AND (
				(
					ISNULL(PermanentAddressProof, '') = ''
					OR ISNULL(PermanentAddressProof, '') LIKE '%PAN%'
					)
				OR (
					PermanentAddressProof LIKE '%Aadhar%'
					AND ISNULL(Aadhaar, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%PAN%'
					AND ISNULL(PAN, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%Passport%'
					AND ISNULL(PassportNumber, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%VoterID%'
					AND ISNULL(VoterIdCard, '') = ''
					)
				OR (
					PermanentAddressProof LIKE '%Driving%'
					AND ISNULL(DrivingLicenseNumber, '') = ''
					)
				)
		
		UNION ALL
		
		SELECT StagingCustom217CustomerFlatId
			,SourceSystemCustomerCode
			,ApplicationRefNumber
			,'CorrespondenceAddressProof address images not found' AS InvalidColRemarks
		FROM Clients.dbo.StagingCustom217CustomerFlat s217 WITH (NOLOCK)
		WHERE s217.SourceSystemName = 'Goldloan'
			AND IsValid = 'Y'
			AND ISNULL(CorrespondenceAddressProof, '') = ''
		) AS T

	UPDATE a
	SET a.IsValid = 'N'
		,a.InvalidColRemarks = t.InvalidColRemarks
	FROM Clients.dbo.StagingCustom217CustomerFlat a WITH (NOLOCK)
	INNER JOIN #tempInValidRecord t ON a.StagingCustom217CustomerFlatId = t.StagingCustom217CustomerFlatId
	WHERE batchId = @BatchId
		AND SourceSystemName = 'Goldloan'
		AND IsValid = 'Y';

	DELETE
	FROM #CustomerImages
	WHERE CUID IN (
			SELECT SourceSystemCustomerCode
			FROM #tempInValidRecord
			);

	--------------------End here Invalid permanant address proof not found 
	INSERT INTO CustomerImagesGL (
		TransactionId
		,FileName
		,FilePath
		,Filetype
		,DocumentType
		,AppRefNoForImages
		,Cuid
		,BinaryFormat
		,Product
		,IsUploaded
		,mkrid
		,AddedOn
		,Batchid
		)
	SELECT ID AS TransactionId
		,REPLACE(REPLACE(NAME, '''', ''), '.png', '.jpg') AS FileName
		,FilePath
		,FileType
		,AttachmentCode AS DocumentType
		,ProspectNo AS AppRefNoForImages
		,CUID
		,BinaryFormat
		,Product
		,'N' AS IsUploaded
		,mkrid
		,GETDATE() AS AddedOn
		,CONVERT(VARCHAR(100), @BatchId) AS Batchid
	FROM (
		SELECT ID
			,FileName AS NAME
			,FilePath
			,LOWER(FileType) AS FileType
			,AttachmentCode
			,ProspectNo
			,CUID
			,mkrid
			,'N' AS BinaryFormat
			,NULL ClientImage
			,'GL' Product
		FROM #CustomerImages
		
		UNION ALL
		
		SELECT CONVERT(VARCHAR(100), a.Srno) AS ID
			,CASE 
				WHEN a.ClientImagePath IS NOT NULL
					THEN RIGHT(a.ClientImagePath, CHARINDEX('\', REVERSE(a.ClientImagePath), 1) - 1)
				ELSE 'Photo_' + ISNULL(a.ProspectNo, '') + '.jpg'
				END AS NAME
			,
			--'\\AZTRUELIES2\CKYC Images\' + scc.SourceSystemCustomerCode + '\Photo_' + isnull(a.prospectno, '') + '.jpg' AS DestFilePath,
			CASE 
				WHEN a.ClientImagePath IS NOT NULL
					THEN a.ClientImagePath
				ELSE '\\AZTRUELIES2\CKYC Images\' + c.CUID + '\Photo_' + ISNULL(a.ProspectNo, '') + '.jpg'
				END AS FilePath
			,CASE 
				WHEN a.ClientImagePath IS NOT NULL
					THEN LOWER('.' + RIGHT(a.ClientImagePath, CHARINDEX('.', REVERSE(a.ClientImagePath)) - 1))
				ELSE '.jpg'
				END AS Filetype
			,'photograph' AS AttachmentCode
			,a.ProspectNo
			,c.CUID CUID
			,MakerID mkrid
			,CASE 
				WHEN a.ClientImagePath IS NULL
					THEN 'Y'
				ELSE 'N'
				END AS BinaryFormat
			,CASE 
				WHEN a.ClientImagePath IS NULL
					THEN a.ClientImage
				ELSE NULL
				END AS ClientImage
			,
			--,convert(VARCHAR(100), @BatchId) AS BatchId
			'GL' Product
		FROM IIFLGOLD.dbo.tbl_IPPhotographs a WITH (NOLOCK)
		INNER JOIN (
			SELECT DISTINCT CUID
				,ProspectNo
			FROM #CustomerImages
			) c ON a.ProspectNo = c.ProspectNo
		) AS FINAL;

	SELECT CONVERT(VARCHAR(100), a.Srno) ID
		,a.FileName AS NAME
		,a.FilePath
		,Filetype
		,DocumentType
		,AppRefNoForImages
		,Cuid
		,mkrid AS MkrID
		,BinaryFormat
		,CASE 
			WHEN a.BinaryFormat = 'Y'
				THEN ip.ClientImage
			ELSE NULL
			END AS ClientImage
		,a.Batchid AS BatchId
		,Product
	FROM CustomerImagesGL a WITH (NOLOCK)
	INNER JOIN IIFLGOLD.dbo.tbl_IPPhotographs ip WITH (NOLOCK) ON a.AppRefNoForImages = ip.ProspectNo
	WHERE Product = 'GL'
		AND IsUploaded = 'N'
		AND a.Batchid = @BatchId
	ORDER BY a.Srno ASC;
END