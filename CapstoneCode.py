# Team 7 Capstone Code for Python by Mychael Solis-Wheeler 8/1/2019

# Import pandas
import pandas as pd

# Note: Each headers in each dataset were checked with spaces removed in each
# Additionally, each dataset were modified as needed, such as removing commas
# in number values to minimize delimiters.

# Variables needed for ease of file access
file_1 = 'MJSalesMonthReports2019.csv'
file_2 = 'MJSalesSummary2019.csv'
file_3 = 'MJTaxMonthReports2019.csv'
file_4 = 'MJTaxSummary2019.csv'
file_5 = 'COCountyPop20142018.csv'
file_6 = 'TXCountyPop2018.csv'
file_7 = 'TX11CityPop20182022.csv'
outpath = '/Users/mykeysw/Desktop/Team7CapstoneFolder/' # May need to update outpath based zipfile downloaded
fileout1 = 'MJCOAvgsRates20142018.csv' 
fileout2 = 'MJTXCountyRevenue2018.csv'
fileout3 = 'MJTX11CityRevenue20182022.csv'

# Use panda data frames to read data from ALL csv files
dfsreport = pd.read_csv(outpath + file_1) 
dfssummary = pd.read_csv(outpath + file_2)  
dftreport = pd.read_csv(outpath + file_3) 
dftsummary = pd.read_csv(outpath + file_4)
dfcocpop = pd.read_csv(outpath + file_5)
dftxcpop = pd.read_csv(outpath + file_6)
dftx11cpop = pd.read_csv(outpath + file_7)

# Check how data is organized in ALL files
dfsreport.head(10)
dfssummary.head(10)
dftreport.head(10)
dftsummary.head(10)
dfcocpop.head(10) 
dftxcpop.head(10)
dftx11cpop.head(10) 
# No delimiters found in ANY files
#---------------------------------------------------------------------------------

# Check for any nulls to fill
dfsreport.count()
dfssummary.count()
dftreport.count()
dftsummary.count()
dfcocpop.count()
dftxcpop.count()
dftx11cpop.count()
# No nulls found in ANY files

# Rename column to City in tx11cpop file
dftx11cpop.rename(columns={'Unnamed: 0':'City'}, inplace=True)
dftx11cpop.count()
#---------------------------------------------------------------------------------

# Drop columns in sreport file not needed  
dfsreport.count() # Look at file count to select columns to drop
dfsreport.drop(columns= ['MSYearToDate'], inplace=True, axis = 1)
dfsreport.drop(columns= ['RSYearToDate'], inplace=True, axis = 1)
dfsreport.drop(columns= ['TSYearToDate'], inplace=True, axis = 1)
dfsreport.drop(columns= ['TotalToDate'], inplace=True, axis = 1)
dfsreport.count() # Look at remaining columns to ensure Month, MedicalSales,
# RetailSales, & TotalSales present only

# Drop columns in ssummary file not needed  
dfssummary.count() # Look at file count to select columns to drop
dfssummary.drop(columns= ['TotalToDate'], inplace=True, axis = 1)
dfssummary.count()  # Look at remaining columns to ensure Month, MedicalSales,
# RetailSales, & TotalSales present only

# Drop columns in treport file not needed  
dftreport.count() # Look at file count to select columns to drop
dftreport.drop(columns= ['RetailSalesTax'], inplace=True, axis = 1)
dftreport.drop(columns= ['RetailExciseTax'], inplace=True, axis = 1)
dftreport.drop(columns= ['SalesTaxFees'], inplace=True, axis = 1)
dftreport.drop(columns= ['YearToDate'], inplace=True, axis = 1)
dftreport.drop(columns= ['TotalToDate'], inplace=True, axis = 1)
dftreport.count()  # Look at remaining columns to ensure Month, SalesTax,
# LicenseFees, & TotalTaxFees present only

# Drop columns in tsummary file not needed  
dftsummary.count() # Look at file count to select columns to drop
dftsummary.drop(columns= ['TotalToDate'], inplace=True, axis = 1)
dftsummary.drop(columns= ['SalesTaxFees'], inplace=True, axis = 1)
dftreport.count()  # Look at remaining columns to ensure Month, SalesTax,
# LicenseFees, SalesTaxFees, & TotalTaxFees present only
#---------------------------------------------------------------------------------

# Convert all month values to years & rename Month column to Year in sreport & treport files
dfsreport.Month = pd.to_datetime(dfsreport.Month).dt.strftime('%Y')
dftreport.Month = pd.to_datetime(dftreport.Month).dt.strftime('%Y')
dfsreport.rename(columns={'Month':'Year'}, inplace=True)
dftreport.rename(columns={'Month':'Year'}, inplace=True)
dfsreport.head(10)
dftreport.head(10)

# Calculating data by groupby commands for file 1 output
# Averaging sales & tax revenue across years
G1 = dfsreport.groupby('Year')['MedicalSales', 'RetailSales', 'TotalSales'].mean().sort_values('Year', ascending = True)
G2 = dftreport.groupby('Year')['SalesTax', 'LicenseFees', 'TotalTaxFees'].mean().sort_values('Year', ascending = True)
G1['AvgMedicalSales'] = G1['MedicalSales']
G1['AvgRetailSales'] = G1['RetailSales']
G1['AvgTotalSales'] = G1['TotalSales']
G1a = G1.drop(['MedicalSales', 'RetailSales','TotalSales'], axis=1)
G2['AvgSalesTax'] = G2['SalesTax']
G2['AvgLicenseFees'] = G2['LicenseFees']
G2['AvgTotalTaxFees'] = G2['TotalTaxFees']
G1a = G1.drop(['MedicalSales', 'RetailSales','TotalSales'], axis=1)
G2a = G2.drop(['SalesTax', 'LicenseFees', 'TotalTaxFees'], axis=1)
G1a.count()
G2a.count()
G1a.head(10)
G2a.head(10)

# Remove NaT row & convert values as integers for cleaner results
G1a.drop('NaT', axis=0, inplace=True)
G1b = G1a.astype(int)
G2b = G2a.astype(int)
#---------------------------------------------------------------------------------

# Create five new dataframes from summed 2014-2018 CO county populations 
# years with dfssummary & dftsummary for merging with file 2
# For 2014 rates from summed 2014 CO populations
dfrsummary1 = dfssummary.iloc[[0],[0]] 
dfrsummary1['MedicalSalesRate'] = ((dfssummary['MedicalSales'] / dfcocpop['2014Pop'].sum())).round(2)
dfrsummary1['RetailSalesRate'] = ((dfssummary['RetailSales'] / dfcocpop['2014Pop'].sum())).round(2)
dfrsummary1['TotalSalesRate'] = ((dfssummary['TotalSales'] / dfcocpop['2014Pop'].sum())).round(2)
dfrsummary1['TXSalesTaxRate'] = ((dfssummary['TotalSales'] * 0.0825 / dfcocpop['2014Pop'].sum())).round(2)
dfrsummary1['SalesTaxRate'] = ((dftsummary['SalesTax'] / dfcocpop['2014Pop'].sum())).round(2)
dfrsummary1['LicenseFeesRate'] = ((dftsummary['LicenseFees'] / dfcocpop['2014Pop'].sum())).round(2)
dfrsummary1['TotalTaxFeesRate'] = ((dftsummary['TotalTaxFees'] / dfcocpop['2014Pop'].sum())).round(2)

# For 2015 rates from summed 2015 CO populations
dfrsummary2 = dfssummary.iloc[[1],[0]] 
dfrsummary2['MedicalSalesRate'] = ((dfssummary['MedicalSales'] / dfcocpop['2015Pop'].sum())).round(2)
dfrsummary2['RetailSalesRate'] = ((dfssummary['RetailSales'] / dfcocpop['2015Pop'].sum())).round(2)
dfrsummary2['TotalSalesRate'] = ((dfssummary['TotalSales'] / dfcocpop['2015Pop'].sum())).round(2)
dfrsummary2['TXSalesTaxRate'] = ((dfssummary['TotalSales'] * 0.0825 / dfcocpop['2015Pop'].sum())).round(2)
dfrsummary2['SalesTaxRate'] = ((dftsummary['SalesTax'] / dfcocpop['2015Pop'].sum())).round(2)
dfrsummary2['LicenseFeesRate'] = ((dftsummary['LicenseFees'] / dfcocpop['2015Pop'].sum())).round(2)
dfrsummary2['TotalTaxFeesRate'] = ((dftsummary['TotalTaxFees'] / dfcocpop['2015Pop'].sum())).round(2)

# For 2016 rates from summed 2016 CO populations
dfrsummary3 = dfssummary.iloc[[2],[0]]
dfrsummary3['MedicalSalesRate'] = ((dfssummary['MedicalSales'] / dfcocpop['2016Pop'].sum())).round(2)
dfrsummary3['RetailSalesRate'] = ((dfssummary['RetailSales'] / dfcocpop['2016Pop'].sum())).round(2)
dfrsummary3['TotalSalesRate'] = ((dfssummary['TotalSales'] / dfcocpop['2016Pop'].sum())).round(2)
dfrsummary3['TXSalesTaxRate'] = ((dfssummary['TotalSales'] * 0.0825 / dfcocpop['2016Pop'].sum())).round(2)
dfrsummary3['SalesTaxRate'] = ((dftsummary['SalesTax'] / dfcocpop['2016Pop'].sum())).round(2)
dfrsummary3['LicenseFeesRate'] = ((dftsummary['LicenseFees'] / dfcocpop['2016Pop'].sum())).round(2)
dfrsummary3['TotalTaxFeesRate'] = ((dftsummary['TotalTaxFees'] / dfcocpop['2016Pop'].sum())).round(2)

# For 2017 rates from summed 2017 CO populations
dfrsummary4 = dfssummary.iloc[[3],[0]]
dfrsummary4['MedicalSalesRate'] = ((dfssummary['MedicalSales'] / dfcocpop['2017Pop'].sum())).round(2)
dfrsummary4['RetailSalesRate'] = ((dfssummary['RetailSales'] / dfcocpop['2017Pop'].sum())).round(2)
dfrsummary4['TotalSalesRate'] = ((dfssummary['TotalSales'] / dfcocpop['2017Pop'].sum())).round(2)
dfrsummary4['TXSalesTaxRate'] = ((dfssummary['TotalSales'] * 0.0825 / dfcocpop['2017Pop'].sum())).round(2)
dfrsummary4['SalesTaxRate'] = ((dftsummary['SalesTax'] / dfcocpop['2017Pop'].sum())).round(2)
dfrsummary4['LicenseFeesRate'] = ((dftsummary['LicenseFees'] / dfcocpop['2017Pop'].sum())).round(2)
dfrsummary4['TotalTaxFeesRate'] = ((dftsummary['TotalTaxFees'] / dfcocpop['2017Pop'].sum())).round(2)

# For 2018 rates from summed 2018 CO populations
dfrsummary5 = dfssummary.iloc[[4],[0]] 
dfrsummary5['MedicalSalesRate'] = ((dfssummary['MedicalSales'] / dfcocpop['2018Pop'].sum())).round(2)
dfrsummary5['RetailSalesRate'] = ((dfssummary['RetailSales'] / dfcocpop['2018Pop'].sum())).round(2)
dfrsummary5['TotalSalesRate'] = ((dfssummary['TotalSales'] / dfcocpop['2018Pop'].sum())).round(2)
dfrsummary5['TXSalesTaxRate'] = ((dfssummary['TotalSales'] * 0.0825 / dfcocpop['2018Pop'].sum())).round(2)
dfrsummary5['SalesTaxRate'] = ((dftsummary['SalesTax'] / dfcocpop['2018Pop'].sum())).round(2)
dfrsummary5['LicenseFeesRate'] = ((dftsummary['LicenseFees'] / dfcocpop['2018Pop'].sum())).round(2)
dfrsummary5['TotalTaxFeesRate'] = ((dftsummary['TotalTaxFees'] / dfcocpop['2018Pop'].sum())).round(2)

# Convert Year columns into string types for all newly created dfs
dfrsummary1['Year'] = dfrsummary1['Year'].astype(str)
dfrsummary2['Year'] = dfrsummary2['Year'].astype(str)
dfrsummary3['Year'] = dfrsummary3['Year'].astype(str)
dfrsummary4['Year'] = dfrsummary4['Year'].astype(str)
dfrsummary5['Year'] = dfrsummary5['Year'].astype(str)
dfrsummary1.head(10)
dfrsummary2.head(10)
dfrsummary3.head(10)
dfrsummary4.head(10)
dfrsummary5.head(10)
dfrsummary1.dtypes
dfrsummary2.dtypes
dfrsummary3.dtypes
dfrsummary4.dtypes
dfrsummary5.dtypes
#---------------------------------------------------------------------------------

# Merge ALL column-edited new dataframes for file 1 output
gmerge1 = G1b.merge(G2b, how= "outer", on='Year') # G1 & G2 inputs merged
gmergeA = dfrsummary1.merge(dfrsummary2, how= "outer", on=['Year','MedicalSalesRate','RetailSalesRate','TotalSalesRate',
                       'TXSalesTaxRate','SalesTaxRate','LicenseFeesRate',
                       'TotalTaxFeesRate']) # gmerge1 & dfrsummary1 inputs merged
gmergeB = gmergeA.merge(dfrsummary3, how= "outer", on=['Year','MedicalSalesRate','RetailSalesRate','TotalSalesRate',
                       'TXSalesTaxRate','SalesTaxRate','LicenseFeesRate',
                       'TotalTaxFeesRate']) # gmerge2 & dfrsummary2 inputs merged
gmergeC = gmergeB.merge(dfrsummary4, how= "outer", on=['Year','MedicalSalesRate','RetailSalesRate','TotalSalesRate',
                       'TXSalesTaxRate','SalesTaxRate','LicenseFeesRate',
                       'TotalTaxFeesRate']) # gmerge3 & dfrsummary3 inputs merged
gmergerates = gmergeC.merge(dfrsummary5, how= "outer", on=['Year','MedicalSalesRate','RetailSalesRate','TotalSalesRate',
                       'TXSalesTaxRate','SalesTaxRate','LicenseFeesRate',
                       'TotalTaxFeesRate']) # gmerge4 & dfrsummary4 inputs merged
gmergedemo = gmergerates.merge(gmerge1, how= "outer", on='Year') # gmerge5 & dfrsummary5 inputs merged
gmergedemo.head(10) # Double check if all columns merged correctly. They did!!!
#---------------------------------------------------------------------------------

# Create new dataframes from selected columns of previous dfrsummary1 with
# (1st year) CO 2014 rates only for eventual file 2 output
# Create variables to multiply with (1st legalized year) CO 2014 rates for tx county pop df
med_sales_rate1 = dfrsummary1.iat[0, 1]
retail_sales_rate1 = dfrsummary1.iat[0, 2]
total_sales_rate1 = dfrsummary1.iat[0, 3]
txsales_tax_rate1 = dfrsummary1.iat[0, 4]
# sales_tax_rate1 = dfrsummary1.iat[0, 5] Excluded for now
license_fees_rate1 = dfrsummary1.iat[0, 6]
total_taxfees_rate1 = dfrsummary1.iat[0, 7]

# From dftxcpop to create new dataframe with (1st legalized year) CO 2014 rates
dftxcpop2 = dftxcpop.loc[:, ['County','Population']]
dftxcpop2['MedicalSales'] = ((med_sales_rate1 * dftxcpop2['Population'])).round(2)
dftxcpop2['RetailSales'] = ((retail_sales_rate1 * dftxcpop2['Population'])).round(2)
dftxcpop2['TotalSales'] = ((total_sales_rate1 * dftxcpop2['Population'])).round(2)
dftxcpop2['TXSalesTax'] = ((txsales_tax_rate1 * dftxcpop2['Population'])).round(2)
#dftxcpop2['SalesTax'] = ((sales_tax_rate1 * dftxcpop2['Population'])).round(2) Excluded for now
dftxcpop2['LicenseFees'] = ((license_fees_rate1 * dftxcpop2['Population'])).round(2)
dftxcpop2['TotalTaxFees'] = ((total_taxfees_rate1 * dftxcpop2['Population'])).round(2)

# Double check new dfs created properly with counts, head, & dtypes
dftxcpop2.count()
dftxcpop2.head(10)
dftxcpop2.dtypes
#---------------------------------------------------------------------------------

# Create new dataframes from selected columns of previous dfrsummaries with
# the other (2nd-5th years) CO 2015-2018 rates for eventual file 3 merged output
# Create variables to multiply with (2nd legalized year) CO 2015 rates for tx 11 city pop df
med_sales_rate2 = dfrsummary2.iat[0, 1]
retail_sales_rate2 = dfrsummary2.iat[0, 2]
total_sales_rate2 = dfrsummary2.iat[0, 3]
txsales_tax_rate2 = dfrsummary2.iat[0, 4]
#sales_tax_rate2 = dfrsummary2.iat[0, 5] Excluded for now
license_fees_rate2 = dfrsummary2.iat[0, 6]
total_taxfees_rate2 = dfrsummary2.iat[0, 7]

# Create variables to multiply with (3rd legalized year) CO 2016 rates for tx 11 city pop df
med_sales_rate3 = dfrsummary3.iat[0, 1]
retail_sales_rate3 = dfrsummary3.iat[0, 2]
total_sales_rate3 = dfrsummary3.iat[0, 3]
txsales_tax_rate3 = dfrsummary3.iat[0, 4]
#sales_tax_rate3 = dfrsummary3.iat[0, 5] Excluded for now
license_fees_rate3 = dfrsummary3.iat[0, 6]
total_taxfees_rate3 = dfrsummary3.iat[0, 7]

# Create variables to multiply with (4th legalized year) CO 2017 rates for tx 11 city pop df
med_sales_rate4 = dfrsummary4.iat[0, 1]
retail_sales_rate4 = dfrsummary4.iat[0, 2]
total_sales_rate4 = dfrsummary4.iat[0, 3]
txsales_tax_rate4 = dfrsummary4.iat[0, 4]
#sales_tax_rate4 = dfrsummary4.iat[0, 5] Excluded for now
license_fees_rate4 = dfrsummary4.iat[0, 6]
total_taxfees_rate4 = dfrsummary4.iat[0, 7]

# Create variables to multiply with (5th legalized year) CO 2018 rates for tx 11 city pop df
med_sales_rate5 = dfrsummary5.iat[0, 1]
retail_sales_rate5 = dfrsummary5.iat[0, 2]
total_sales_rate5 = dfrsummary5.iat[0, 3]
txsales_tax_rate5 = dfrsummary5.iat[0, 4]
#sales_tax_rate5 = dfrsummary5.iat[0, 5] Excluded for now
license_fees_rate5 = dfrsummary5.iat[0, 6]
total_taxfees_rate5 = dfrsummary5.iat[0, 7]

# From dftx11cpop to create new dataframes with (1st to 5th legalized years) CO 2014-2018 rates
# For top TX 11 city 2018Pop with (1st legalized year) CO 2014 rates
dftx11cpop1 = dftx11cpop.loc[:, ['City','2018Pop']]
dftx11cpop1['2018MedicalSales'] = ((med_sales_rate1 * dftx11cpop1['2018Pop'])).round(2)
dftx11cpop1['2018RetailSales'] = ((retail_sales_rate1 * dftx11cpop1['2018Pop'])).round(2)
dftx11cpop1['2018TotalSales'] = ((total_sales_rate1 * dftx11cpop1['2018Pop'])).round(2)
dftx11cpop1['2018TXSalesTax'] = ((txsales_tax_rate1 * dftx11cpop1['2018Pop'])).round(2)
#dftx11cpop1['2018SalesTax'] = ((sales_tax_rate1 * dftx11cpop1['2018Pop'])).round(2) Excluded for now
dftx11cpop1['2018LicenseFees'] = ((license_fees_rate1 * dftx11cpop1['2018Pop'])).round(2)
dftx11cpop1['2018TotalTaxFees'] = ((total_taxfees_rate1 * dftx11cpop1['2018Pop'])).round(2)

# For top TX 11 city 2019Pop with (2nd legalized year) CO 2015 rates
dftx11cpop2 = dftx11cpop.loc[:, ['City','2019Pop']]
dftx11cpop2['2019MedicalSales'] = ((med_sales_rate2 * dftx11cpop2['2019Pop'])).round(2)
dftx11cpop2['2019RetailSales'] = ((retail_sales_rate2 * dftx11cpop2['2019Pop'])).round(2)
dftx11cpop2['2019TotalSales'] = ((total_sales_rate2 * dftx11cpop2['2019Pop'])).round(2)
dftx11cpop2['2019TXSalesTax'] = ((txsales_tax_rate2 * dftx11cpop2['2019Pop'])).round(2)
#dftx11cpop2['2019SalesTax'] = ((sales_tax_rate2 * dftx11cpop2['2019Pop'])).round(2) Excluded for now
dftx11cpop2['2019LicenseFees'] = ((license_fees_rate2 * dftx11cpop2['2019Pop'])).round(2)
dftx11cpop2['2019TotalTaxFees'] = ((total_taxfees_rate2 * dftx11cpop2['2019Pop'])).round(2)

# For top TX 11 city 2020Pop with (3rd legalized year) CO 2016 rates
dftx11cpop3 = dftx11cpop.loc[:, ['City','2020Pop']]
dftx11cpop3['2020MedicalSales'] = ((med_sales_rate3 * dftx11cpop3['2020Pop'])).round(2)
dftx11cpop3['2020RetailSales'] = ((retail_sales_rate3 * dftx11cpop3['2020Pop'])).round(2)
dftx11cpop3['2020TotalSales'] = ((total_sales_rate3 * dftx11cpop3['2020Pop'])).round(2)
dftx11cpop3['2020TXSalesTax'] = ((txsales_tax_rate3 * dftx11cpop3['2020Pop'])).round(2)
#dftx11cpop3['2020SalesTax'] = ((sales_tax_rate3 * dftx11cpop3['2020Pop'])).round(2) Excluded for now
dftx11cpop3['2020LicenseFees'] = ((license_fees_rate3 * dftx11cpop3['2020Pop'])).round(2)
dftx11cpop3['2020TotalTaxFees'] = ((total_taxfees_rate3 * dftx11cpop3['2020Pop'])).round(2)

# For top TX 11 city 2021Pop with (4th legalized year) CO 2017 rates
dftx11cpop4 = dftx11cpop.loc[:, ['City','2021Pop']]
dftx11cpop4['2021MedicalSales'] = ((med_sales_rate4 * dftx11cpop4['2021Pop'])).round(2)
dftx11cpop4['2021RetailSales'] = ((retail_sales_rate4 * dftx11cpop4['2021Pop'])).round(2)
dftx11cpop4['2021TotalSales'] = ((total_sales_rate4 * dftx11cpop4['2021Pop'])).round(2)
dftx11cpop4['2021TXSalesTax'] = ((txsales_tax_rate4 * dftx11cpop4['2021Pop'])).round(2)
#dftx11cpop4['2021SalesTax'] = ((sales_tax_rate4 * dftx11cpop4['2021Pop'])).round(2) Excluded for now
dftx11cpop4['2021LicenseFees'] = ((license_fees_rate4 * dftx11cpop4['2021Pop'])).round(2)
dftx11cpop4['2021TotalTaxFees'] = ((total_taxfees_rate4 * dftx11cpop4['2021Pop'])).round(2)

# For top TX 11 city 2022Pop with (5th legalized year) CO 2018 rates
dftx11cpop5 = dftx11cpop.loc[:, ['City','2022Pop']]
dftx11cpop5['2022MedicalSales'] = ((med_sales_rate5 * dftx11cpop5['2022Pop'])).round(2)
dftx11cpop5['2022RetailSales'] = ((retail_sales_rate5 * dftx11cpop5['2022Pop'])).round(2)
dftx11cpop5['2022TotalSales'] = ((total_sales_rate5 * dftx11cpop5['2022Pop'])).round(2)
dftx11cpop5['2022TXSalesTax'] = ((txsales_tax_rate5 * dftx11cpop5['2022Pop'])).round(2)
#dftx11cpop5['2022SalesTax'] = ((sales_tax_rate5 * dftx11cpop5['2022Pop'])).round(2) Excluded for now
dftx11cpop5['2022LicenseFees'] = ((license_fees_rate5 * dftx11cpop5['2022Pop'])).round(2)
dftx11cpop5['2022TotalTaxFees'] = ((total_taxfees_rate5 * dftx11cpop5['2022Pop'])).round(2)

# Double check new dfs created properly with counts, head, & dtypes
dftx11cpop1.count()
dftx11cpop2.count()
dftx11cpop3.count()
dftx11cpop4.count()
dftx11cpop5.count()
dftx11cpop1.head(10)
dftx11cpop2.head(10)
dftx11cpop3.head(10)
dftx11cpop4.head(10)
dftx11cpop5.head(10)
dftx11cpop1.dtypes
dftx11cpop2.dtypes
dftx11cpop3.dtypes
dftx11cpop4.dtypes
dftx11cpop5.dtypes
#---------------------------------------------------------------------------------

# Merge ALL dftx11cop1 thru dftx11cop5 new dataframes for file 3 output
tx11merge1 = dftx11cpop1.merge(dftx11cpop2, how= "outer", on='City') # dftx11cpop1 & dftx11cpop2 inputs merged
tx11merge2 = tx11merge1.merge(dftx11cpop3, how= "outer", on='City') # tx11merge1 & dftx11cpop3 inputs merged
tx11merge3 = tx11merge2.merge(dftx11cpop4, how= "outer", on='City') # tx11merge2 & dftx11cpop4 inputs merged
tx11mergedemo = tx11merge3.merge(dftx11cpop5, how= "outer", on='City') # tx11merge3 & dftx11cpop5 inputs merged
tx11mergedemo.head(10) # Double check if all columns merged correctly. They did!!!
#---------------------------------------------------------------------------------

# Drop 2019 row as it is not needed in gmergedemo (for file 1 output)
gmergedemo.drop([5], axis=0, inplace=True)
gmergedemo.count()
gmergedemo.head(10) # Double check removed correctly. 2019 row was removed!!!

# Nothing to drop in dftxcpop2 (for file 2 output)
dftxcpop2.count()

# Nothing to drop in tx11mergedemo (for file 3 output)
tx11mergedemo.count()
#--------------------------------------------------------------------------------

# Double check if there are duplicates in gmergedemo, dftxcpop2, tx11mergedemo
gmergedemo[gmergedemo.duplicated(['Year'], keep='first')]
dftxcpop2[dftxcpop2.duplicated(['County'], keep='first')]
tx11mergedemo[tx11mergedemo.duplicated(['City'], keep='first')]
# No rows were found to be duplicated more than once for any 
#---------------------------------------------------------------------------------

# Create csv files from all final file outputs
# For MJCOAvgsRates20142018.csv file 1 output 
MJCOAvgsRates20142018 = gmergedemo
MJCOAvgsRates20142018.to_csv(outpath + fileout1, index=False) # No number index with columns included

# For MJTXCountyRevenue2018.csv file 2 output
MJTXCountyRevenue2018 = dftxcpop2
MJTXCountyRevenue2018.to_csv(outpath + fileout2, index=False) # No number index with columns included

# For MJTX11CityRevenue20182022.csv file 3 output
MJTX11CityRevenue20182022 = tx11mergedemo
MJTX11CityRevenue20182022.to_csv(outpath + fileout3, index=False) # No number index with columns included
