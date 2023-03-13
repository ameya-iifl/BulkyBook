;WITH CTE_image AS
		(
			SELECT
				ROW_NUMBER() OVER (PARTITION BY tkd.CUID, tkd.FamilyId ORDER BY tkd.srno DESC) AS rn
				,tkd.CUID AS CUID
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
				,tkd.Srno AS TransactionID
				,CASE  WHEN tkd.image_path LIKE '%\%' THEN    RIGHT(tkd.image_path, CHARINDEX('\', REVERSE(tkd.image_path), -1) - 1)  ELSE '' END AS FileName
				,tkd.image_path AS FilePath
				,CASE WHEN tkd.image_path LIKE '%.%' THEN REVERSE(SUBSTRING(REVERSE(tkd.image_path), 1, CHARINDEX('.', REVERSE(tkd.image_path)) - 1)) END FileType
				,tkd.mkrid AS mkrid
			FROM IIFLGOLD.dbo.tbl_KYCdoc_Details tkd WITH (NOLOCK)
			INNER JOIN IIFLGOLD.dbo.Tbl_DocumentMaster tdm WITH (NOLOCK) ON tdm.Srno = tkd.DocumentID
			WHERE  
			(tkd.FamilyID IN (7,8) AND tkd.IsKYCDoc = 'Y')
			AND tkd.DocumentID>0
			AND tkd.image_path IS NOT NULL
		)

INSERT INTO CustomerImagesAll
        (
			BusinessCode
            ,TransactionId
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
      --      ,Batchid
        )
SELECT 
	'GL' AS BusinessCode
	,CTE.TransactionID
	,CTE.FileName
	,CTE.FilePath
	,CTE.FileType
	,CTE.AttachmentCode AS DocumentType
	,s217.ApplicationRefNumber AS AppRefNoForImages
	,CTE.CUID
	,'N' AS BinaryFormat
	,'GL' AS Product
	,'N' AS IsUploaded
	,CTE.mkrid
	,GETDATE() AS AddedOn
--	,CONVERT(VARCHAR(100), @BatchId) AS Batchid


FROM dbo.StagingCustom217CustomerFlat s217 WITH(NOLOCK)
INNER JOIN CTE 
    ON CTE.CUID = s217.SourceSystemCustomerCode
 WHERE s217.IsValid = 'Y'
    AND CTE.AttachmentCode!=''
    AND CTE.rn=1


INSERT INTO CustomerImagesAll
        (
			BusinessCode
            ,TransactionId
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
      --      ,Batchid
        )

SELECT 
	'GL'
	,CONVERT(VARCHAR(100), A.Srno) AS TransactionID
	,CASE WHEN A.ClientImagePath IS NOT NULL THEN RIGHT(A.ClientImagePath, CHARINDEX('\', REVERSE(A.ClientImagePath), 1) - 1)
               ELSE 'Photo_' + ISNULL(A.ProspectNo, '') + '.jpg' END AS FileName
	,CASE WHEN A.ClientImagePath IS NOT NULL THEN A.ClientImagePath
               ELSE '\\AZTRUELIES2\CKYC Images\' + tkd.CUID + '\Photo_' + ISNULL(A.ProspectNo, '') + '.jpg' END AS FilePath
	,CASE WHEN A.ClientImagePath IS NOT NULL THEN LOWER('.' + RIGHT(a.ClientImagePath, CHARINDEX('.', REVERSE(a.ClientImagePath)) - 1))
               ELSE '.jpg' END AS Filetype
	,'photograph' AS DocumentType
	,A.ProspectNo 
	,tkd.CUID 
	,CASE WHEN A.ClientImagePath IS NULL THEN 'Y'
               ELSE 'N' END AS BinaryFormat
	,'GL' Product
	,'N' AS IsUploaded
	,A.MakerID
	,GETDATE() AS AddedOn
    ,CONVERT(VARCHAR(100), @BatchId) AS Batchid
		
FROM IIFLGOLD.dbo.tbl_IPPhotographs A WITH (NOLOCK)
     INNER JOIN dbo.StagingCustom217CustomerFlat s217 WITH(NOLOCK)
		 ON A.ProspectNo=s217.ApplicationRefNumber
	 INNER JOIN IIFLGOLD.dbo.tbl_KYCdoc_Details tkd WITH (NOLOCK)
		ON s217.SourceSystemCustomerCode=tkd.CUID
	 INNER JOIN IIFLGOLD.dbo.Tbl_DocumentMaster tdm WITH (NOLOCK) 
		ON tdm.Srno = tkd.DocumentID
	WHERE  
			(tkd.FamilyID IN (7,8) AND tkd.IsKYCDoc = 'Y')
			AND tkd.DocumentID>0
			AND tkd.image_path IS NOT NULL 
			AND s217.IsValid='Y'





          