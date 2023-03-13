--;WITH CTE
--AS
--(
--SELECT ROW_NUMBER() OVER (PARTITION BY B.CUID, B.FamilyID ORDER BY B.Srno DESC) AS rn
--,B.Srno AS TransactionId
--,B.image_path as Filepath
--,B.mkrid
--,B.DocumentID
--,B.CUID
--,B.ImageName
--,B.FamilyID

--,CASE  WHEN B.image_path LIKE '%\%' THEN    RIGHT([B.image_path], CHARINDEX('\', REVERSE([B.image_path]), -1) - 1)  ELSE '' END AS FileName
--,CASE WHEN B.image_path LIKE '%.%' THEN REVERSE(SUBSTRING(REVERSE(B.image_path), 1, CHARINDEX('.', REVERSE(B.image_path)) - 1)) END AS FileType
--,CASE
--                       WHEN D.AttachmentCode LIKE '%aadharcard%' THEN
--                           'AadharCard'
--                       WHEN D.AttachmentCode LIKE '%pan%' THEN
--                           'PAN'
--                       WHEN D.AttachmentCode LIKE '%voter%' THEN
--                           'VoterID'
--                       WHEN D.AttachmentCode LIKE '%passport%' THEN
--                           'Passport'
--                       WHEN D.AttachmentCode LIKE '%driving%' THEN
--                           'DrivingLicence'
--                       ELSE
--                           ISNULL(AttachmentCode, '')
--                   END AttachmentCode

--FROM dbo.StagingCustom217CustomerFlat A
--JOIN IIFLGOLD.dbo.tbl_KYCdoc_Details B
--ON A.SourceSystemCustomerCode=B.CUID
--JOIN IIFLGOLD.dbo.Tbl_DocumentMaster C
--ON B.DocumentID=C.Srno
--JOIN Clients.dbo.tbl_DocMappingMaster D
--ON B.FamilyID=D.FamilyID
--	AND C.Srno=D.Srno
--WHERE A.IsValid = 'Y'
--                  AND B.image_path IS NOT NULL
--                  AND DocumentID > 0
--                  AND (( B.FamilyID IN ( 7, 8) AND B.IsKYCDoc = 'Y') OR (B.FamilyID = 17))
--	AND A.batchId=@batchId
--	AND A.SourceSystemName = 'Goldoan'
--)

--UPDATE A
--PermanentAddressProof = 
--,ProofOfIDSubmitted=
--,CorrespondenceAddressProof=
--FROM dbo.StagingCustom217CustomerFlat A
--JOIN IIFLGOLD.dbo.tbl_KYCdoc_Details B
--ON A.SourceSystemCustomerCode=B.CUID
--JOIN IIFLGOLD.dbo.Tbl_DocumentMaster C
--ON B.DocumentID=C.Srno
--WHERE A.IsValid = 'Y'
--                  AND B.image_path IS NOT NULL
--                  AND DocumentID > 0
--                  AND (( B.FamilyID IN ( 7, 8) AND B.IsKYCDoc = 'Y') OR (B.FamilyID = 17))
--	--AND A.batchId=@batchId
--	--AND A.SourceSystemName = 'Goldoan'
	

;WITH CTE AS
(
	SELECT
		ROW_NUMBER() OVER (PARTITION BY tkd.CUID ORDER BY tkd.srno DESC) AS rn
		,tkd.CUID
		,CASE 
				WHEN tkd.FamilyID = 7
					AND tdm.Srno = 17
					THEN 'passport' --and FamilyDescription ='Passport' 
				WHEN tkd.FamilyID = 7
					AND tdm.Srno = 53
					THEN 'aadharcard' -- and FamilyDescription ='Aadhaar Card'
				WHEN tkd.FamilyID = 7
					AND tdm.Srno = 54
					THEN 'drivinglicense' --and FamilyDescription ='Driving License'
				WHEN tkd.FamilyID = 7
					AND tdm.Srno = 55
					THEN 'voter' --and FamilyDescription ='Election ID Card'
				WHEN tkd.FamilyID = 8
					AND tdm.Srno = 56
					AND FamilyDescription = 'Aadhaar Card'
					THEN 'aadharcard'
				WHEN tkd.FamilyID = 8
					AND tdm.Srno = 22
					AND FamilyDescription = 'Passport'
					THEN 'passport'
				WHEN tkd.FamilyID = 8
					AND tdm.Srno = 23
					AND FamilyDescription = 'Driving License'
					THEN 'drivinglicense'
				WHEN tkd.FamilyID = 8
					AND tdm.Srno = 24
					AND FamilyDescription = 'Election ID Card'
					THEN 'voter'
				WHEN tkd.FamilyID = 8
					AND tdm.Srno = 25
					AND FamilyDescription = 'PAN Card'
					THEN 'pan'
				ELSE ''
				END AS AttachmentCode
	FROM IIFLGOLD.dbo.tbl_KYCdoc_Details tkd WITH (NOLOCK)
	INNER JOIN IIFLGOLD.dbo.Tbl_DocumentMaster tdm WITH (NOLOCK) ON tdm.Srno = tkd.DocumentID
	WHERE  (tkd.FamilyID IN (7,8) AND tkd.IsKYCDoc = 'Y')
)

UPDATE Cus
SET CUS.PermanentAddressProof = CTE.AttachmentCode
	,ProofOfIDSubmitted = CTE.AttachmentCode
	,CUS.CorrespondenceAddressProof = CTE.AttachmentCode
FROM Clients.dbo.StagingCustom217CustomerFlat  Cus
INNER JOIN CTE ON CTE.CUID = Cus.SourceSystemCustomerCode
WHERE --BatchId = @BatchId AND 
	Cus.SourceSystemName = 'Goldloan'
	AND CUS.IsValid = 'Y'
	AND ISNULL(CTE.AttachmentCode, '') != ''
	AND CTE.AttachmentCode <> 'pan'
	AND CTE.rn = 1;