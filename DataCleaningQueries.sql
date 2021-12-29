

---Cleaning Data Queries---

------------------------------------------------
--Standardizing Dates--
------------------------------------------------
/*
SELECT SaleDate
FROM DataCleaning..NashvilleHousing

BEGIN TRAN
ALTER TABLE Datacleaning..NashvilleHousing
ADD Temp DATE 
UPDATE DataCleaning..NashvilleHousing
SET Temp = CONVERT(DATE, SaleDate)
UPDATE DataCleaning..NashvilleHousing
SET SaleDate = Temp
ALTER TABLE Datacleaning..NashvilleHousing 
DROP COLUMN Temp
SELECT temp
FROM DataCleaning..NashvilleHousing

-- ROLLBACK TRAN
-- COMMIT TRAN
*/

BEGIN TRAN
ALTER TABLE Datacleaning..NashvilleHousing
ALTER COLUMN SaleDate DATE
SELECT SaleDate
FROM DataCleaning..NashvilleHousing
COMMIT TRAN

------------------------------------------------
--Populate Property Address Data--
------------------------------------------------

SELECT *
FROM DataCleaning..NashvilleHousing
--WHERE PropertyAddress IS NULL
ORDER BY ParcelID

SELECT LeftSide.ParcelID, Leftside.PropertyAddress, Rightside.ParcelID, Rightside.PropertyAddress, ISNULL(Leftside.PropertyAddress, RightSide.PropertyAddress) AS Handoff
FROM DataCleaning..NashvilleHousing AS LeftSide
JOIN DataCleaning..NashvilleHousing AS RightSide
	ON LeftSide.ParcelID = RightSide.ParcelID
	AND Leftside.[UniqueID ] <> RightSide.[UniqueID ]
	WHERE Leftside.PropertyAddress IS NULL

UPDATE LeftSide
SET PropertyAddress = ISNULL(Leftside.PropertyAddress, RightSide.PropertyAddress)
FROM DataCleaning..NashvilleHousing AS LeftSide
JOIN DataCleaning..NashvilleHousing AS RightSide
	ON LeftSide.ParcelID = RightSide.ParcelID
	AND Leftside.[UniqueID ] <> RightSide.[UniqueID ]



------------------------------------------------
--Deconstructing Address--
------------------------------------------------

SELECT PropertyAddress
FROM DataCleaning..NashvilleHousing

SELECT 
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress)-1) AS ADDRESS,
SUBSTRING(PropertyAddress, (CHARINDEX(',', PropertyAddress)+1), LEN(PropertyAddress)) AS CITY 
FROM DataCleaning..NashvilleHousing

BEGIN TRAN 
ALTER TABLE NashvilleHousing
ADD FinalPropertyAddress NVARCHAR(255)
UPDATE NashvilleHousing
SET FinalPropertyAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress)-1)


ALTER TABLE NashvilleHousing
ADD FinalPropertyCity NVARCHAR(255)
UPDATE NashvilleHousing
SET FinalPropertyCity = SUBSTRING(PropertyAddress, (CHARINDEX(',', PropertyAddress)+1), LEN(PropertyAddress))

ALTER TABLE NashvilleHousing
DROP COLUMN PropertyAddress

SELECT * 
FROM DataCleaning..NashvilleHousing











------------------------------------------------
--Deconstructing Owner Address--
------------------------------------------------
SELECT OwnerAddress 
FROM DataCleaning..NashvilleHousing

SELECT PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3) AS OAddress,
PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2) AS OCity,
PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1) AS OState
FROM DataCleaning..NashvilleHousing

ALTER TABLE DataCleaning..NashvilleHousing
ADD OwnAddress NVARCHAR(255)

UPDATE DataCleaning..NashvilleHousing 
SET OwnAddress = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3)

ALTER TABLE DataCleaning..NashvilleHousing
ADD OwnCity NVARCHAR(255)

UPDATE DataCleaning..NashvilleHousing 
SET OwnCity = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2)

ALTER TABLE DataCleaning..NashvilleHousing
ADD OwnState NVARCHAR(255)

UPDATE DataCleaning..NashvilleHousing 
SET OwnState = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1)

SELECT OwnAddress, OwnCity, OwnState
FROM DataCleaning..NashvilleHousing 

---------------------------------------------------------
----Replacing Y and N to Yes and No in 'SoldasVacant'----
---------------------------------------------------------



SELECT SoldAsVacant,
CASE 
	WHEN SoldAsVacant = 'Y' THEN 'Yes'
	WHEN SoldAsVacant = 'N' THEN 'No'
	ELSE SoldAsVacant
END AS 'Corrected Y/N'
FROM Datacleaning..NashvilleHousing

UPDATE Datacleaning..NashvilleHousing
SET SoldAsVacant = 
	CASE 
		WHEN SoldAsVacant = 'Y' THEN 'Yes'
		WHEN SoldAsVacant = 'N' THEN 'No'
		ELSE SoldAsVacant
	END


SELECT DISTINCT(SoldAsVacant), COUNT(SoldAsVacant)
FROM DataCleaning..NashvilleHousing
GROUP BY SoldAsVacant
ORDER BY 2

---------------------------------------------------------
---------------Removing Duplicates-----------------------
---------------------------------------------------------

SELECT *,
	ROW_NUMBER() 
	OVER (
		PARTITION BY ParcelID, 
					 PropertyAddress,
					 SaleDate,
					 LegalReference
					 ORDER BY
						UniqueID
		) AS Rownum
FROM DataCleaning..NashvilleHousing
ORDER BY ParcelID

CREATE TABLE #TEMP_Nashville_Excluding_Duplicates (

UniqueID INT,
ParcelID NVARCHAR (255),
LandUse NVARCHAR (255),
PropertyAddress NVARCHAR (255),
SaleDate DATE,
SalePrice INT,
LegalReference NVARCHAR (255),
SoldAsVacant NVARCHAR (255),
OwnerName NVARCHAR (255),
OwnerAddress NVARCHAR (255),
Acreage FLOAT, 
TaxDistrict NVARCHAR (255),
LandValue INT,
BuildingValue INT,
TotalValue INT,
YearBuilt INT,
 Bedrooms INT,
FullBath INT, 
HalfBath INT,
OwnAddress NVARCHAR (255),
OwnCity NVARCHAR (255),
OwnState NVARCHAR (255))
 
SELECT * 
FROM #TEMP_Nashville_Excluding_Duplicates

INSERT INTO #TEMP_Nashville_Excluding_Duplicates
SELECT *
FROM DataCleaning..NashvilleHousing

--DROP TABLE #TEMP_Nashville_Excluding Duplicates 

WITH RowNumCTE AS (
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID, 
				 PropertyAddress,
				 SaleDate,
				 LegalReference
				 ORDER BY
						UniqueID
						) AS Rownum
FROM #TEMP_Nashville_Excluding_Duplicates
)
SELECT*
FROM RowNumCTE
WHERE Rownum > 1;
/*
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID, 
				 PropertyAddress,
				 SaleDate,
				 LegalReference
				 ORDER BY
						UniqueID
						) AS Rownum
FROM #TEMP_Nashville_Excluding_Duplicates
*/

WITH ProofRowNumCTE AS (
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID, 
				 PropertyAddress,
				 SaleDate,
				 LegalReference
				 ORDER BY
						UniqueID
						) AS Rownum
FROM DataCleaning..NashvilleHousing)
SELECT *
FROM ProofRowNumCTE
WHERE Rownum > 1

---------------------------------------------------------
------------Removing Unneeded Columns--------------------
---------------------------------------------------------

ALTER TABLE #TEMP_Nashville_Excluding_Duplicates 
DROP COLUMN OwnerAddress, TaxDistrict

SELECT * 
FROM #TEMP_Nashville_Excluding_Duplicates