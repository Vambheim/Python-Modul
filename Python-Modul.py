#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 19:46:31 2023

@author: palvambheim
"""

import pandas as pd
import mysql.connector
import json

df = pd.read_csv("DATAGRUNNLAG",  index_col = "Unnamed: 0")
                
# df['ABG Sundal'] = df['ABG Sundal'].str.replace('−', '-')
# df['ABG Sundal'] = df['ABG Sundal'].astype(float)

#Fjerner tomrom i skjema.
df = df.dropna(axis = 0, how = "all")
#Fjerner kolonner med for mange manglende verdier. 
df = df.dropna(axis = 1, thresh = 15)
#Fyller tomme celler med 0
df.fillna(0, inplace=True)
#Sletter kolonne "Unit" da denne ikke trengs. Skulle den trengs kan denne linjen fjernes
#og alle splicer må settes til [2:]
df = df.drop(columns=["Unit"])


df = df.astype(str)

#Remove empty spaces between values: 
df = df.applymap(lambda x: x.replace('\xa0', '').replace(' ', '') if isinstance(x, str) else x)


# Replace non-standard negative sign with regular minus sign
df = df.apply(lambda x: x.str.replace('−', '-'))

# Convert all columns except the first one to float

df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')

# Check the data types of the columns
print(df.dtypes)

#Retrieve the name of each column
column_names = df.columns[1:].to_numpy()

futureYears = 7


class Company:
    def __init__(self, NetIncome, Taxcosts, Netfinancingexpenses, Depreciation_amortization, 
                 UnrealisedGain_netinterestexpense, Operationalleaseadjustment, Depreciationofleaseadjustment, 
                 Taxespaid, CAPEX, ChangeNWC, WACC, EBITDAexitmultiple, Existingdebt, Operatingleaseadjustment, 
                 Unfundedpensionliabilities, Currentcash, ImpliedFVofnoncontrollinginterests, Outstandingnumberofshares, Shareprice):
        self.NetIncome = NetIncome
        self.Taxcosts = Taxcosts
        self.Netfinancingexpenses = Netfinancingexpenses
        self.Depreciation_amortization = Depreciation_amortization
        self.UnrealisedGain_netinterestexpense = UnrealisedGain_netinterestexpense
        self.Operationalleaseadjustment = Operationalleaseadjustment
        self.Depreciationofleaseadjustment = Depreciationofleaseadjustment
        self.Taxespaid = Taxespaid
        self.CAPEX = CAPEX
        self.ChangeNWC = ChangeNWC
        self.WACC = WACC
        self.EBITDAexitmultiple = EBITDAexitmultiple
        self.Existingdebt = Existingdebt
        self.Operatingleaseadjustment = Operatingleaseadjustment
        self.Unfundedpensionliabilities = Unfundedpensionliabilities
        self.Currentcash = Currentcash
        self.ImpliedFVofnoncontrollinginterests = ImpliedFVofnoncontrollinginterests
        self.Outstandingnumberofshares = Outstandingnumberofshares
        self.Shareprice = Shareprice

    def __str__(self):
           return f"Company(EBITDA={self.calculate_EBITDA()}, OCFBT={self.calculate_OCFBT()}, CFBT={self.calculate_CFBT()}, PFCF={self.calculate_PFCF()}, UFCF={self.calculate_UFCF()}, discount={self.calculate_discount()}, presentvalue={self.calculate_presentvalue()}, future_EBITDA={self.future_EBITDA(7)}, future_changeNWC={self.future_changeNWC(7)}, futureOCFBT={self.futureOCFBT(7)}, futureCAPEX={self.future_CAPEX(7)}, future_CFBT={self.future_CFBT(7)}, future_taxespaid={self.future_taxespaid(7)}, future_PFCF={self.future_PFCF(7)}, future_opLease={self.future_opLeaseAdjustment(7)}, future_UFCF={self.future_UFCF(7)}, futureDiscount={self.futureDiscount(8, discount)}, presentvalueUFCF={self.presentValueUFCF(8, discount)}, EBITDAResult={self.EBITDAresult(7)}, futureDiscount2027={self.futureDiscount2027(7, discount)}, discountedTermnialValue={self.discountedTerminalValue(7, discount)}, sumOfUFCF={self.SumOfUFCF(8, discount)}, enterpriseValue={self.enterpriseValue()}, impliedEquityValue={self.impliedEquityValue()}, impliedEquityofCommonStockholders={self.ImpliedEquityofCommonStockholders()}, amountOfShares = {self.amountShares()} valuePerShare={self.valuePerShare()}, Original Value Per Share: = {self.currentSharePrice()})"
           #return f"Company(EBITDA={self.calculate_EBITDA()}, OCFBT={self.calculate_OCFBT()}, CFBT={self.calculate_CFBT()}, PFCF={self.calculate_PFCF()}, UFCF={self.calculate_UFCF()}, discount={self.calculate_discount()}, presentvalue={self.calculate_presentvalue()}, future_EBITDA={full_EBITDA[-1]}, future_changeNWC={full_changeNWC[-1]}, futureOCFBT={full_OpCashFlowBeforeTax[-1]}, futureCAPEX={full_CAPEX[-1]}, future_CFBT={full_CFBT[-1]}, future_taxespaid={full_taxespaid[-1]}, future_PFCF={full_PFCF[-1]}, future_opLease={full_opLeaseAdjustment[-1]}, future_UFCF={full_UFCF[-1]}, futureDiscount={full_Discount[-1]}, presentvalueUFCF={fullPresentValueUFCF[-1]}, EBITDAResult={self.EBITDAresult(7)}, futureDiscount2027={self.futureDiscount2027(7, self.calculate_discount())}, discountedTermnialValue={self.discountedTerminalValue(7, self.calculate_discount())}, sumOfUFCF={self.SumOfUFCF(8, self.calculate_discount())}, enterpriseValue={self.enterpriseValue()}, impliedEquityValue={self.impliedEquityValue()}, impliedEquityofCommonStockholders={self.ImpliedEquityofCommonStockholders()}, amountOfShares={self.amountShares()}, valuePerShare={self.valuePerShare()}, Original Value Per Share:={self.currentSharePrice()})"

    def calculate_EBITDA(self):
            global EBITDA
            EBITDA = self.NetIncome + self.Taxcosts + self.Netfinancingexpenses + self.Depreciation_amortization + self.UnrealisedGain_netinterestexpense
            return EBITDA


#Operational Cash Flow Before Tax (OCFBT)
    def calculate_OCFBT(self):
            global OCFBT
            #Change in pensions is N/A so it is not taken into account
            OCFBT = EBITDA + self.ChangeNWC
            return OCFBT
    
        
    #Cash Flow Beore tax (CFBT)
    def calculate_CFBT(self):
            global CFBT
            CFBT = OCFBT + self.CAPEX
            return CFBT
    
    #Pre-financing Cash Flow (PFCF)
 
    def calculate_PFCF(self): 
            global PFCF
            PFCF = CFBT + self.Taxespaid
            return PFCF
    
    #Unlevered Free Cash Flow (UFCF)
    def calculate_UFCF(self):
            global UFCF
            UFCF = PFCF + self.Operationalleaseadjustment + self.Depreciationofleaseadjustment
            return UFCF
    
    def calculate_discount(self): 
            global discount
            try:
                discount = 1 / (1 + (float(self.WACC)/100))
            except ValueError:
                print("Error: WACC must be a numerical value")
                discount = None
            return discount
    
    #Present Value of unlevered FCF
    def calculate_presentvalue(self):
        global presentvalue
        self.calculate_discount()
        self.calculate_UFCF()
        if discount is not None:
            presentvalue = UFCF * discount
            return presentvalue
        else:
            print("Error: Could not calculate presentvalue, discount is None")
            return None

    
    
    #Calculate future EBITDA based on EBITDA exitmultiple as increasePerYear
    def future_EBITDA(self, years): 
            global future_EBITDA
            future_EBITDA = []
            for year in range(years)[1:]:
                #future_EBITDA.append(EBITDA * (1 + (self.EBITDAexitmultiple/100))**year)
                future_EBITDA.append(EBITDA * (1 + (self.EBITDAexitmultiple/100))**year)
            return future_EBITDA
    
        #Calculate change in working capital
    def future_changeNWC(self, years): 
            future_changeNWC= []
            for year in range(years)[1:]:
                future_changeNWC.append(self.ChangeNWC * (1 + (self.EBITDAexitmultiple/100))**year)
            return future_changeNWC
   
    #Calculate future Cash Flow Before Tax
    def futureOCFBT(self, years):
            future_EBITDA = self.future_EBITDA(years)
            future_changeNWC = self.future_changeNWC(years)
            futureOCFBT = []
            for i in range(len(future_EBITDA)):
                futureOCFBT.append(future_EBITDA[i] + future_changeNWC[i])
            return futureOCFBT
    
    #Future CAPEX
    def future_CAPEX(self, years):
            global future_CAPEX
            future_CAPEX = []
            for year in range(years)[1:]:
                future_CAPEX.append(self.CAPEX * (1 + (self.EBITDAexitmultiple/100))**year)
            return future_CAPEX
    
    #Future Cash flow before taxes
    def future_CFBT(self, years): 
            global future_CFBT
            future_CAPEX = self.future_CAPEX(years)
            future_OCFBT = self.futureOCFBT(years)
            future_CFBT = []
            for i in range(len(future_OCFBT)):
                future_CFBT.append(future_CAPEX[i] + future_OCFBT[i])
            return future_CFBT
    
    #Future taxes paid
    def future_taxespaid(self, years): 
            global future_taxespaid
            future_taxespaid = []
            for year in range(years)[1:]:
                future_taxespaid.append(self.Taxespaid *(1 + (self.EBITDAexitmultiple/100))**year)
            return future_taxespaid
   
    #Future Pre Financing Cash Flow
    def future_PFCF(self, years):
           global future_PFCF
           future_CFBT = self.future_CFBT(years)
           future_taxespaid = self.future_taxespaid(years)
           future_PFCF = []
           for i in range(len(future_CFBT)):
               future_PFCF.append(future_CFBT[i] + future_taxespaid[i])
           return future_PFCF
    
    #Future Operational Lease adjustment
    def future_opLeaseAdjustment(self, years):
            global future_opLeaseAdjustment
            future_opLeaseAdjustment = []
            for year in range(years)[1:]:
                future_opLeaseAdjustment.append(self.Operationalleaseadjustment *(1 + (self.EBITDAexitmultiple/100))** year)
            return future_opLeaseAdjustment
    
    #Depreciation of lease adjustment 
    def future_depLeaseAdjustment(self, years):
        global future_depLeaseAdjustment
        future_depLeaseAdjustment = []
        for year in range(years)[1:]:
            future_depLeaseAdjustment.append(self.Depreciationofleaseadjustment * (1 + (self.EBITDAexitmultiple/100))**year)
        return future_depLeaseAdjustment
    
    
    #Depreciation of lease adjustment is not included as data is N/A
    
    #Future Unleveraged Free Cash Flow
    def future_UFCF(self, years):
            global future_UFCF
            future_PFCF= self.future_PFCF(years)
            future_opLeaseAdjustment = self.future_opLeaseAdjustment(years)
            future_depLeaseAdjustment = self.future_depLeaseAdjustment(years)
            future_UFCF = []
            for i in range(len(future_PFCF)):
                future_UFCF.append(future_depLeaseAdjustment[i] + future_opLeaseAdjustment[i] + future_PFCF[i])
            return future_UFCF
    
   # def calculate_discount(self): 
        #global discount
        #discount = 1 / (1 + (self.WACC/100))
        #return discount
    
    
    #Future discount
    def futureDiscount(self, years, discount): 
            global futureDiscount 
            futureDiscount = []
            for year in range(years)[2:]:
                futureDiscount.append((discount)**year)
            return futureDiscount
    
        
    #Present value Unlevered Free Cash Flow
    def presentValueUFCF(self, years, discount):
            global presentvalueUFCF
            future_ufcf = self.future_UFCF(years)
            future_discount = self.futureDiscount(years, discount)
            result = []
            for i in range(len(future_discount)):
                result.append(future_ufcf[i] * future_discount[i])
            return result
    
    
#Table 2 Method 1

   #EBITDA In 2027 multiplied by Exit Multiple
    def EBITDAresult(self, years):
            future_EBITDA_list = self.future_EBITDA(years)
            exitmultiple = self.EBITDAexitmultiple
            global result
            result = future_EBITDA_list[-1] * exitmultiple
            return result

   
    
    #Future discount in 2027
    def futureDiscount2027(self, years, discount): 
            global discount_in_2027
            discount_in_2027 = (self.calculate_discount())**(years)
            return discount_in_2027
    
    
    def discountedTerminalValue(self, years, discount):
            global discountedTV
            discountedTV = self.futureDiscount2027(years, discount) * result
            return discountedTV
        

    #Present value Unlevered Free Cash Flow
    def SumOfUFCF(self, years, discount):
        global SumOfUFCF
        future_ufcf = self.future_UFCF(years)
        future_discount = self.futureDiscount(years, discount)
        result = []
        for i in range(len(future_discount) - 1):
            result.append((future_ufcf[i] * future_discount[i]))
        SumOfUFCF = sum(result)
        SumOfUFCF = SumOfUFCF + presentvalue
        return SumOfUFCF


        
        #Total enterprise value in 2027
    def enterpriseValue(years):
            global enterpriseValue
            enterpriseValue = SumOfUFCF + discountedTV
            return enterpriseValue
    
    def impliedEquityValue(self):
            existingDebt = self.Existingdebt
            #OPERATING lease adusjtment
            opLeaseAdjustment = self.Operatingleaseadjustment
            unfundedPension = self.Unfundedpensionliabilities
            currentCash = self.Currentcash
            global impliedEquityValue
        
            impliedEquityValue = enterpriseValue + existingDebt + opLeaseAdjustment + unfundedPension + currentCash
            return impliedEquityValue
        
    def ImpliedEquityofCommonStockholders(self):
            ImpliedFVofnoncontrollinginterests =  self.ImpliedFVofnoncontrollinginterests 
            global impliedEquityofCommonStockholders
            impliedEquityofCommonStockholders = impliedEquityValue + ImpliedFVofnoncontrollinginterests
            return impliedEquityofCommonStockholders
    
    def valuePerShare(self):
            nrOfShares = self.Outstandingnumberofshares
            valuePerShare = impliedEquityofCommonStockholders / nrOfShares
            return valuePerShare
    
    def currentSharePrice(self):
        currentSharePrice = self.Shareprice
        return currentSharePrice
    def amountShares(self):
        amountOfShares = self.Outstandingnumberofshares
        return amountOfShares
  

# create empty list to store company instances

companies = []


# iterate over columns
for column in df.columns[1:]:
    # create new company instance with values from column
    company_instance = Company(*[row[column] for index, row in df.iterrows()])
    # add company instance to list
    companies.append(company_instance)

    
     
# Print company name and financials.
for i, col in enumerate(column_names):
    company_instance = Company(*[row[i+1] for index, row in df.iterrows()])
    company_instance.columns = [col]
    print("Company name:", company_instance.columns[0])
    print("Company financials", company_instance)

    
    



industry_id =  1
# Connect to MySQL
mydb = mysql.connector.connect(
  host="DATABASEHOST",
  user="DATABASEBRUKER",
  password="DATABASEPASSORD",
  database="DATABASENAVN"
)


mycursor = mydb.cursor()


#Create a table for the companies entity if it doesn't already exist
#mycursor.execute("CREATE TABLE IF NOT EXISTS company (company_id INT AUTO_INCREMENT PRIMARY KEY, company_name VARCHAR(255), EBITDTA FLOAT, OCFBT FLOAT, CFBT FLOAT, PFCF FLOAT, UFCF FLOAT, future_EBITDA FLOAT, future_changeNWC FLOAT, futureOCFBT FLOAT, future_CAPEX FLOAT, future_CFBT FLOAT, future_taxespaid FLOAT, future_PFCF FLOAT, future_opLeaseAdjustment FLOAT, future_depLeaseAdjustment FLOAT, future_UFCF FLOAT, futureDiscount FLOAT, presentvalueUFCF FLOAT, result FLOAT, discount_in_2027 FLOAT, discountedTV FLOAT, SumOfUFCF FLOAT, enterpriseValue FLOAT, impliedEquityValue FLOAT, ImpliedEquityOfCommonStockholders FLOAT, valuePerShare FLOAT, CurrentSharePrice FLOAT, amountShares INT)")

# Loop through your final results and insert the data into the companies table
# Print company name and financials.
for i, col in enumerate(column_names):
    company_instance = Company(*[row[i+1] for index, row in df.iterrows()])
    company_instance.columns = [col]
    val = (
    company_instance.columns[0],
    industry_id,
    company_instance.calculate_EBITDA(),
    company_instance.calculate_OCFBT(),
    company_instance.calculate_CFBT(),
    company_instance.calculate_PFCF(),
    company_instance.calculate_UFCF(),

    json.dumps(company_instance.calculate_discount()),
    json.dumps(company_instance.calculate_presentvalue()),
    json.dumps(company_instance.future_EBITDA(futureYears)),
    json.dumps(company_instance.future_changeNWC(futureYears)),
    json.dumps(company_instance.futureOCFBT(futureYears)),
    json.dumps(company_instance.future_CAPEX(futureYears)),
    json.dumps(company_instance.future_CFBT(futureYears)),
    json.dumps(company_instance.future_taxespaid(futureYears)),
    json.dumps(company_instance.future_PFCF(futureYears)),
    json.dumps(company_instance.future_depLeaseAdjustment(futureYears)),
    json.dumps(company_instance.future_opLeaseAdjustment(futureYears)),
    json.dumps(company_instance.future_UFCF(futureYears)),
    json.dumps(company_instance.futureDiscount(8, discount)),
    json.dumps(company_instance.presentValueUFCF(futureYears, discount)),

    company_instance.EBITDAresult(futureYears),
    company_instance.futureDiscount2027(futureYears, discount),
    company_instance.discountedTerminalValue(futureYears, discount),
    company_instance.SumOfUFCF(8, discount),
    company_instance.enterpriseValue(),
    company_instance.impliedEquityValue(),
    company_instance.ImpliedEquityofCommonStockholders(),
    company_instance.currentSharePrice(),
    company_instance.amountShares(),
    company_instance.valuePerShare()
    
       #     return f"Company(EBITDA={self.calculate_EBITDA()}, OCFBT={self.calculate_OCFBT()}, 
       # CFBT={self.calculate_CFBT()}, PFCF={self.calculate_PFCF()}, UFCF={self.calculate_UFCF()}, 
       # discount={self.calculate_discount()}, presentvalue={self.calculate_presentvalue()}, future_EBITDA={self.future_EBITDA(7)}, 
       # future_changeNWC={self.future_changeNWC(7)}, futureOCFBT={self.futureOCFBT(7)}, futureCAPEX={self.future_CAPEX(7)}, 
       # future_CFBT={self.future_CFBT(7)}, future_taxespaid={self.future_taxespaid(7)}, future_PFCF={self.future_PFCF(7)}, 
       # future_opLease={self.future_opLeaseAdjustment(7)}, future_UFCF={self.future_UFCF(7)}, futureDiscount={self.futureDiscount(8, discount)}, 
       # presentvalueUFCF={self.presentValueUFCF(8, discount)}, EBITDAResult={self.EBITDAresult(7)}, futureDiscount2027={self.futureDiscount2027(7, discount)},
       # discountedTermnialValue={self.discountedTerminalValue(7, discount)}, sumOfUFCF={self.SumOfUFCF(8, discount)}, 
       # enterpriseValue={self.enterpriseValue()}, 
       # impliedEquityValue={self.impliedEquityValue()}, impliedEquityofCommonStockholders={self.ImpliedEquityofCommonStockholders()}, amountOfShares = {self.amountShares()} valuePerShare={self.valuePerShare()}, Original Value Per Share: = {self.currentSharePrice()})"

    
    )
    print(val)

    sql = "INSERT INTO company (company_name, industry_id, EBITDA, OCFBT, CFBT, PFCF, UFCF, discount, presentvalue, future_EBITDA, future_changeNWC, futureOCFBT, future_CAPEX, future_CFBT, future_taxespaid, future_PFCF, future_depLeaseAdjustment, future_opLeaseAdjustment, future_UFCF, futureDiscount, presentValueUFCF, EBITDAresult, futureDiscount2027, discountedTerminalValue, PVofUnleveredFCF, enterpriseValue, impliedEquityValue, ImpliedEquityOfCommonStockholders, currentSharePrice, amountShares, calculated_value_per_share) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.execute(sql, val)
    



# Commit the changes and close the connection to the database
mydb.commit()
mydb.close()

# print the number of inserted rows
print(mycursor.rowcount, "record(s) inserted.")




        
        
    

            
