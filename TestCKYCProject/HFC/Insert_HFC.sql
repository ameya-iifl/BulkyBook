ELSE IF @product='HFC'

BEGIN

---insert for Applicant
WITH CTE
		AS (
			SELECT CUID
				,ProspectNo
				,BusinessCode
				,ROW_NUMBER() OVER (
					PARTITION BY CUID ORDER BY CreatedOn DESC
					) AS rn
			FROM ISOM.LOANS.PrimaryApplicant PA WITH (NOLOCK)
			LEFT OUTER JOIN dbo.StagingCustom217CustomerFlat S WITH (NOLOCK) ON S.SourceSystemCustomerCode = PA.CUID
			WHERE BusinessCode = 'HFC'
				AND S.SourceSystemCustomerCode IS NULL
				AND PA.LoanStatus = 'ACT'
				AND PA.ProspectNo LIKE "IL%"
			)
		INSERT INTO Clients.dbo.StagingCustom217CustomerFlat (
			ParentCompany
			--,
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
			,ActivitySector
			,NatureOfBusiness
			,NatureOfBusinessOther
			,DateofBirth
			,WorkEmail
			,PersonalEmail
			,KYCVerificationBranch
			,KYCEmployeeCode
			,PermanentCKYCAddressType
			,PlotnoSurveynoHouseFlatno
			,PermanentAddressCountry

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
			,HolderforImages -- check
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
			,FORMSixty
			,NPRLetter
			,KYCAttestationType
			,BatchId
			,IsValid

		)
		SELECT 
			'HFC1' AS ParentCompany
			--,
			,'HFC' AS SourceSystemName
			,PA.CUID AS SourceSystemCustomerCode
			,PA.CreatedOn AS SourceSystemCustomerCreationDate
			,0 AS IsSmallCustomer
			,0 AS EkycOTPbased
			,'' RecordIdentifier
			,'' Segments
			,'' SegmentStartDate
			,'' ProductSegments
			,CASE WHEN PA.LoanStatus = 'ACT' THEN 'Active'
							WHEN PA.LoanStatus = 'CLS' THEN 'Closed'
							ELSE 'InActive' END AS CustomerStatus
			,'' RelatedPartyStatus 
			,'' RelatedPartyStatusEffectiveDate
			,1 AS CustomerType
			,'' CustomerSubType
			,PA.Prefix
			,PA.FirstName
			,PA.MiddleName
			,PA.LastName
			,'' MaidenPrefix
			,'' MaidenFirstName
			,'' MaidenMiddleName
			,'' MaidenLastName
			,'Mr'
			,FI.FatherFirstName
			,FI.FatherMiddleName
			,FI.FatherLastName
			,FI.SpouseTitle
			,FI.SpouseFirstName
			,FI.SpouseMiddleName
			,FI.SpouseLastName
			,'Mrs'
			,FI.MotherFirstName
			,FI.MotherMiddleName
			,FI.MotherLastName
			,PA.Gender
			,FI.MaritalStatus
			,'IN' AS Citizenship
			,'' AS CountryOfResidence
			,'' ActivitySector
			,'' NatureOfBusiness
			,'' NatureOfBusinessOther
			,PA.DateOfBirth
			,'' WorkEmail
			,'' PersonalEmail
			,'HO' KYCVerificationBranch
			,PA.CreatedOn AS KYCEmployeeCode
			,01 AS PermanentCKYCAddressType
			,'' PlotnoSurveynoHouseFlatno
			,'IN' PermanentAddressCountry

			,'' AS WorkAddressCountry
			,'' AS WorkAddressPinCode
			,'' AS WorkAddressLine1
			,'' AS WorkAddressLine2
			,'' AS WorkAddressLine3
			,'' AS WorkAddressDistrict
			,'' AS WorkAddressCity
			,'' AS WorkAddressState
			,'IN' AS CountryOfBirth
			,NULL AS BirthCity
			,'IN' AS TaxResidencyCountry
			,NULL AS TaxIdentificationNumber
			,'IN' AS TaxResidencyAddressCountry
			,'' AS TaxResidencyAddressLine1
			,'' AS TaxResidencyAddressLine2
			,'' AS TaxResidencyAddressLine3
			,'' AS TaxResidencyAddressPinCode
			,'' AS TaxResidencyAddressDistrict
			,'' AS TaxResidencyAddressCity
			,'' AS TaxResidencyAddressState
			,'' AS DeskPersonalISDCode
			,NULL AS DeskPersonalSTDCode
			,NULL AS DeskPersonalTelephoneNumber
			,NULL AS DeskWorkISDCode
			,NULL AS DeskWorkSTDCode
			,NULL AS DeskWorkTelephoneNumber
			,'' AS WorkMobileISD
			,'' AS WorkMobileNumber
			,NULL AS PersonalMobileISD
			,PA.PrimaryMobileNumber AS PersonalMobileNumber
			,'' AS CKYCID
			,'IN' AS PassportIssueCountry
			,PA.PassportNumber AS PassportNumber
			,PA.PassportNumberExpiry AS PassportExpiryDate
			,'' AS VoterIdCard -- why??
			,PA.PAN AS PAN
			,PA.DrivingLicenseNumber AS  DrivingLicenseNumber
			,PA.DrivingLicenseExpiry AS DrivingLicenseExpiryDate
			,PA.AadharNumber AS Aadhaar
			,'' AS AadhaarVaultReferenceNumber
			,'' AS AadhaarToken
			,'' AS AadhaarVirtualId
			,'' AS NREGA
			,'' AS CKYCPOIOtherCentralGovtID
			,'' AS CKYCPOIS01IDNumber
			,'' AS CKYCPOIS02IDNumber
			,'' AS NationalID
			,'' AS TaxIdLocal
			,'' AS CompanyRegistrationNumber
			,'' AS CompanyRegistrationCountry
			,'' AS GIIN
			,'' AS OthersInd
			,'' AS OthersNonInd
			,'' AS ProofOfIDSubmitted
			,0 AS Minor
			,PA.ProspectNo AS ApplicationRefNumber
			,HolderforImages -- check
			-- ,IntermediaryCode **insert this in update clause 
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
			,PA.CreatedBy AS AddedBy
			,GETDATE() AS AddedOn
			,'' AS GUID
			,CASE 
					WHEN ISNULL(PA.PAN, '') != ''
						THEN 0
					ELSE 1
					END AS FORMSixty
			,'' AS NPRLetter
			,'03' AS KYCAttestationType
			,@batchId AS BatchId
			,'Y' AS IsValid
		FROM ISOM.LOANS.PrimaryApplicant PA WITH (NOLOCK)
				--JOIN ISOM.LOANS.SecondaryParticipant SP WITH (NOLOCK)
				--ON PA.ProspectNo=SP.ProspectNo
				JOIN ISOM.LOANS.FamilyInformation FI WITH (NOLOCK) ON PA.ProspectNo = FI.ProspectNo
					AND PA.BusinessCode = FI.BusinessCode
				JOIN ISOM.LOANS.LoanInfo LI WITH (NOLOCK) ON FI.ProspectNo = LI.ProspectNo
					AND FI.BusinessCode = LI.BusinessCode
				JOIN CTE ON CTE.ProspectNo = PA.ProspectNo
					AND CTE.BusinessCode = PA.BusinessCode
				WHERE PA.BusinessCode = 'HFC'

---insert for co-Applicant (Co-Borrower)
		INSERT INTO Clients.dbo.StagingCustom217CustomerFlat (
			ParentCompany
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





			,Citizenship
			,CountryOfResidence

			,ActivitySector
			,NatureOfBusiness
			,NatureOfBusinessOther
			,DateofBirth
			,WorkEmail
			,PersonalEmail
			,KYCDateOfDeclaration


			,KYCVerificationBranch

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

			,CKYCID
			,PassportIssueCountry

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
			,NPRLetter
			,KYCAttestationType
			,batchId
			,IsValid
		)
		SELECT 
			'HFC1' ParentCompany
			,'HFC' SourceSystemName
			,SourceSystemCustomerCode
			,SP.CreatedOn AS SourceSystemCustomerCreationDate
			,0 AS IsSmallCustomer
			,0 AS EkycOTPbased
			,'' RecordIdentifier
			,'' Segments
			,'' SegmentStartDate
			,'' ProductSegments
			,CASE WHEN PA.LoanStatus = 'ACT'
						THEN 'Active'
				  WHEN PA.LoanStatus = 'CLS'
						THEN 'Closed'
					ELSE 'InActive'
			END AS CustomerStatus
			,SP.CreatedOn
			,'' RelatedPartyStatus
			,'' RelatedPartyStatusEffectiveDate
			,1 AS CustomerType
			,'' CustomerSubType
			,SP.Title AS Prefix
			,SP.FirstName AS FirstName
			,SP.MiddleName AS MiddleName
			,SP.LastName AS LastName
			,'' MaidenPrefix
			,'' MaidenFirstName
			,'' MaidenMiddleName
			,'' MaidenLastName



			,'IN' Citizenship
			,'' CountryOfResidence

			,'' ActivitySector
			,'' NatureOfBusiness
			,'' NatureOfBusinessOther
			,SP.DateOfBirth
			,'' WorkEmail
			,'' PersonalEmail
			,SP.CreatedOn AS KYCDateofDeclaration

			,'HO' KYCVerificationBranch


			,'' WorkAddressCountry
			,'' WorkAddressPinCode
			,'' WorkAddressLine1
			,'' WorkAddressLine2
			,'' WorkAddressLine3
			,'' WorkAddressDistrict
			,'' WorkAddressCity
			,'' WorkAddressState
			,'IN' CountryOfBirth
			,'' BirthCity
			,'IN' TaxResidencyCountry
			,'' TaxIdentificationNumber
			,'IN' TaxResidencyAddressCountry
			,'' TaxResidencyAddressLine1
			,'' TaxResidencyAddressLine2
			,'' TaxResidencyAddressLine3
			,'' TaxResidencyAddressPinCode
			,'' TaxResidencyAddressDistrict
			,'' TaxResidencyAddressCity
			,'' TaxResidencyAddressState
			,'' DeskPersonalISDCode
			,NULL AS DeskPersonalSTDCode
			,NULL AS DeskPersonalTelephoneNumber
			,'' DeskWorkISDCode
			,NULL AS DeskWorkSTDCode
			,NULL AS DeskWorkTelephoneNumber
			,'' WorkMobileISD
			,'' WorkMobileNumber
			,NULL AS PersonalMobileISD

			,NULL AS CKYCID
			,'IN' PassportIssueCountry

			,'' AadhaarVaultReferenceNumber
			,'' AadhaarToken
			,'' AadhaarVirtualId
			,'' NREGA
			,'' CKYCPOIOtherCentralGovtID
			,'' CKYCPOIS01IDNumber
			,'' CKYCPOIS02IDNumber
			,'' NationalID
			,'' TaxIdLocal
			,'' CompanyRegistrationNumber
			,'' CompanyRegistrationCountry
			,'' GIIN
			,'' OthersInd
			,'' OthersNonInd
			,'' ProofOfIDSubmitted
			,0 AS Minor
			,SP.ProspectNo AS ApplicationRefNumber
			,'' HolderforImages
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
			,'CKYC' ModuleApplicable
			,SP.CreatedBy AS AddedBy
			,GETDATE() AS AddedOn
			,'' GUID 

			,'' NPRLetter
			,'03' KYCAttestationType
			,@BatchId AS BatchId
			,'Y' AS IsValid

		FROM ISOM.LOANS.PrimaryApplicant PA WITH (NOLOCK)
				JOIN ISOM.LOANS.SecondaryParticipant SP WITH (NOLOCK)
				ON PA.ProspectNo=SP.ProspectNo
				JOIN ISOM.LOANS.FamilyInformation FI WITH (NOLOCK) ON PA.ProspectNo = FI.ProspectNo
					AND PA.BusinessCode = FI.BusinessCode
				JOIN ISOM.LOANS.LoanInfo LI WITH (NOLOCK) ON FI.ProspectNo = LI.ProspectNo
					AND FI.BusinessCode = LI.BusinessCode
				JOIN CTE ON CTE.ProspectNo = PA.ProspectNo
					AND CTE.BusinessCode = PA.BusinessCode
				JOIN IILHFC.dbo.IndividualMaster IM WITH (NOLOCK) ON PA.ProspectNo=IM.Ind_ProspectNo
				WHERE PA.BusinessCode = 'HFC'
				AND SP.ApplicantType='COBORROWER'


		UPDATE A
		SET A.PermanentCKYCAddressType = LA.AddressType
			,A.PlotnoSurveynoHouseFlatno = ''
			,A.PermanentAddressCountry = 'IN'
			,A.PermanentAddressPinCode = LA.Pincode
			,A.PermanentAddressLine1 = LA.AddressLine1
			,A.PermanentAddressLine2 = LA.AddressLine2
			,A.PermanentAddressLine3 = LA.AddressLine3
			,A.PermanentAddressDistrict = ''
			,A.PermanentAddressCity = VA.City
			,A.PermanentAddressState = LA.STATE
		FROM dbo.StagingCustom217CustomerFlat A
		INNER JOIN ISOM.Loans.Addresses LA ON A.ApplicationRefNumber = LA.ProspectNo
			AND LA.BusinessCode = 'HFC'
			AND LA.AddressType = 'CURRENT RESIDENCE'
		INNER JOIN dbo.view_Addresses_refer VA ON A.ApplicationRefNumber = VA.ad_ProspectNo
		WHERE A.BatchId = @batchid
			AND A.ApplicationRefNumber = LA.ProspectNo
			AND LEFT(A.TransactionID, 2) = 'HFC';
		
		---update contact details address to correspondence addresses------ 
		UPDATE B
		SET B.CorrespondenceAddressCountry = 'IN'
			,B.CorrespondenceAddressPinCode = LA.Pincode
			,B.CorrespondenceAddressLine1 = LA.AddressLine1
			,B.CorrespondenceAddressLine2 = LA.AddressLine2
			,B.CorrespondenceAddressLine3 = LA.AddressLine3
			,B.CorrespondenceAddressDistrict = ''
			,B.CorrespondenceAddressCity = VA.City
			,B.CorrespondenceAddressState = LA.STATE
		FROM dbo.StagingCustom217CustomerFlat B
		INNER JOIN ISOM.Loans.Addresses LA WITH (NOLOCK) ON B.ApplicationRefNumber = LA.ProspectNo
			AND LA.BusinessCode = 'HFC'
			AND LA.AddressType = 'CURRENT OFFICE'
		INNER JOIN dbo.view_Addresses_refer VA ON B.ApplicationRefNumber = VA.ad_ProspectNo
		WHERE B.BatchId = @batchid
			AND B.ApplicationRefNumber = LA.ProspectNo
			AND LEFT(B.TransactionID, 2) = 'HFC';

END -- END OF IF @PRODUCT=HFC