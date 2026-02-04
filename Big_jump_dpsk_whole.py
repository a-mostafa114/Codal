# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 13:35:40 2025

@author: vapa2530
"""


import Levenshtein
import os
from datetime import datetime, timedelta
import pandas as pd
import tqdm
import json
import time
import tiktoken
from typing import Dict, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo
import string
import unicodedata
from rapidfuzz  import process, fuzz
import re
import regex
import pyreadstat
from collections import defaultdict

#Some functions recursively used
def remove_accents(s): #function that removes accents within a string
    if isinstance(s, str):
        return ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )
    return s

def fuzzy_match_rapidfuzz(x, choices): #Function that defines the matching process
    match = process.extractOne(x, choices, scorer=fuzz.token_sort_ratio )
    return pd.Series(match) 

def clean_edges(txt: str) -> str:
    return regex.sub(r'^[\p{P}\p{Zs}]+|[\p{P}\p{Zs}]+$', '', txt)

def complete_first_word(partial, full_line): #Function that extracts and completes the first word, used for mathces
    match = re.search(r'\b' + re.escape(partial) + r'[^\s,\.]*', full_line)
    if match:
        return match.group()
    return partial

def adj_unmatch(row):
    ln = row.get("last_name", "")
    if not isinstance(ln, str):
        ln = ""

    base_ln = ln.split()[0] if ln else ""

    if base_ln in set(df_death_reg_unacc["last_name"].dropna()):
        ln = base_ln

    parts = ln.split()
    if len(parts) > 0 and parts[0] == "V.":
        ln = ln.replace("V.", "Von")

    if len(ln.split()) > 1:
        row["last_name"] = ln.split()[0]
    else:
        row["last_name"] = ln

    return row

#Dirty last names list
dirty_last_names_list = pd.read_excel("Last_names_to_update_DR.xlsx", sheet_name="Sheet2")
dirty_last_names_list["last_name"] = dirty_last_names_list["line"].str.split(",", n= 1).str[0].str.strip()
dirty_last_names_list = dirty_last_names_list[["last_name","last_name_clean"]].drop_duplicates(subset = "last_name")
dirty_last_names_dict = dirty_last_names_list.set_index("last_name")["last_name_clean"].to_dict()




#The updated death register that we are going to use
df_death_reg_unacc = pd.read_csv("Updated_DR.csv")
df_death_reg_unacc = df_death_reg_unacc.sort_values(by="last_name", key=lambda x: x.str.len(),ascending=False)
df_death_reg_unacc = df_death_reg_unacc[~df_death_reg_unacc["last_name"].str.contains("hustru")]

#Import first names
first_names, meta = pyreadstat.read_dta("Burial_names.dta")
del meta
first_names = first_names["firstname"].drop_duplicates()
first_names= (
    first_names
    .str.replace("ö", "o")
    .str.replace("ä", "a")
    .str.replace("à", "a")
    .str.replace("å", "a")
    .str.replace("Ö", "O")
    .str.replace("Ä", "A")
    .str.replace("Å","A")
    .str.replace("Ü","U")
)


#Import the file with the year that you need 
# df_1912 = pd.read_csv("taxeringskalender_1912_02.csv")
df_1912 = pd.read_csv("dpsk_whole.csv")
main_dataframe = df_1912.copy()

#Words which are not an occupation
no_occ_list = ["hustru", "fru", "fröken", "änkefru"]


#Create the main dataframe on whiche we are going to operate
main_dataframe["no_occ"] = 0
surname_list = main_dataframe[["page","column","row","line"]]
surname_list["line"] = surname_list["line"].apply(remove_accents) #Remove accents in the line
surname_list["matched"] = False #Starting point
surname_list = surname_list[surname_list["line"].apply(lambda x: isinstance(x, str))] #Transform every line in a string 
surname_list["last_name"] = "" #Create an empty column 


#Adjust O instead of 0
clean_O = surname_list[surname_list["line"].str.contains(r'\b0,\s',regex = True)]
clean_O["line"] = clean_O["line"].apply(lambda x: re.sub(r'0,\s', r'O., ', x))
surname_list.update(clean_O)

##################################################
#First define a firm pattern
pattern = r'Sparkassa|Pharmacia|Produktkompaniet|Norra Frivilliga Arbetshuset|Mellersta & Norra Sveriges Angpannefor- ening|Siosteens|Social-Demokraten|Maleriarbetarforbundet|Missionsforbunaet|Metallindustriarbetareforbundet|Landtmannens Riksforbund|Traarbetareforbundet|Tvalkompaniet|Norra Station|Pilgrimstads Andersmejeri|AB|Machinery|Exportaffar|Centralautomaten|Pram- & Bogs|Sagverksforbundet|Bryggeriidkareforbundet|Credit|Sallskapet|Elektriska|Handelsbanken|Pappersbruk|Sjomanshemmet|\bKredit|Sprithandelsbol|\bAkt\.|Timmermansorden|Tomtrattskassa|Hypotekskassa|Societe General|Schlesische Feuerversicherungs|Rante- och Kapitalforsakringsanstalten|Olycksfallsforsakr\.|Hotell|Centralbanken|Banque|Laval Separator|United Shoe|Forlagsexpedition|Accumuslatoren|Affarssystem|Affarsbanken|Gesellschaft|servicekassa|Spirituosabol|Assurance-Comp|Afdeln|Spritforsaljningsbol|Mjolkcentral|Tegelindustri|C:o|Industriforbund|Express Comp|Elektricitats-Ges|Coldinu Orden|Transmissionsverken|Pensionsfond|National Versicherungs|Advokatsamfund|Publicistklubben|Generaldepot|Lanekassa|Generaldepot|C:o Limited|Pupillkassan|olycksfallsforsakringsanstalten|Lmtd|Kreditkassa|laskedrycksfabr\.|generaldepot|pensionsfond|Olycksfallforsakringsanstalten|Stora Sallskapet|Stadernas Allmanna|forsamlingen|hamnarbetskontor|hypotekskassa|brandforsakringskontor|Schweizerische Unfallversicherungs-A.-G. Kh., 16850-16800|Commercial Union|Elektricitetsv.|Elektr\.-verk|A\s*\.B-\.|samfundet|Petroleum|A\.\s*-B|organisationen|Stads|Centralforbundet|-verk,|Hartzlimfabr|Cementgjuteri|fabriksbod|Borgerskapskassa|intressenter|Korkfabrik|filial|Angbryggeri|Lysoljeaffar|Yllefabrik|verket|hofding gre|Allm\.|Byra|Kungl\.|Foreningen|Armaturfabriken|Forenade Industrier|besparingsskog|jarnvagsdrift|Brandforsakringsinrattn|tradgardinfabriken|Andels|haradsallmanning|u\.p\.\a|Nya Ullspinneri|Petroleumselskab|Goteborgssystemet|Hushallsskolan|Bolag|Stadspark|Sparbanken|firma|sparkasse|stiftelse|villastad|tomtrattsk|arbetshuset|foren|kaffebranneri|Insurance|Bolaget|Banken|u\. p\. a\.|stationer|A\.B\.|Company|Ltd|Filial|Hogfjallspensionat|jarnvag(?![A-Za-z])|Jarnvag(?![A-Za-z])|Koop\.|Kooperativa|Gasverket|Mjolkforsaljn|Vattenledning|\b[A-Za-z]{2,}sverk[^A-Za-z]+|\b[A-Za-z]{5,}fabrik\b|Bank(?![A-Za-z])|A-B|A- B|-B\.|A\.-B\.|c:o|A-\.B|Svenska|svenska|forening|Sthlms|sthlms|-akt\.-bol\.|-akt\.|akt\.-|-b\.|societ|aktie|Aktie|-bol|bol\.|bolag|Bostad|bank(?![A-Za-z])|L:td|a\.-b\.|akt\.-bol\.|fonden'

#Parishes and cities which will be used for later cleaning
cities_par = ["Asarum","Alfvesta","Rimbo","Herrljunga","Kopenhamn","Kungsor","Karlskrona","Saltsjobaden","Saltsjobaden","Sundbyberg","Harene",
    "Stockholms","Haga","Skon","Hoby","Bracke","Ahus","Svardsjo","Vinslof","Hogsby","Ekby","Billeberga","Mellosa","Morlunda",
    "Stockholm","Hvena","Kyrkefalla","Sandby","Lidingo",
    "Liljeholmen","Stentorp",
    "Rimbo","Alno",
    "Saltsjobaden",
    "Sundbyberg",
    "Stockholms stad",
    "Botkyrka",
    "Goteborg",
    "Karlskrona",
    "Sthlm",  
    "Kopenhamn",  
    "Kyrkhult",
    "Asarum",
    "Hjortsberga",
    "Visby",
    "Hogran",
    "Voxna",
    "Loos",
    "Orgryte",
    "Smedsasen",
    "Malilla",
    "Finja",
    "Hassleholm",
    "Perstorp",
    "Farlof",
    "Glimakra",
    "Hjarsa",
    "Brosarp",
    "Hastveda",
    "Elmhult",
    "Alfvesta",
    "Asheda",
    "Bjuf",
    "Raus",
    "Skraflinge",
    "Korpilombolo",
    "Jukkasjarvi",
    "Morko",
    "Bettna",
    "Oxelosund",
    "Vrena",
    "By",
    "Kungsor",
    "Tranemo",
    "Herrljunga",
    "Nasby",
    "Almby",
    "Eggby",
    "Kyrkefalla"]

#Parish exact notation
parish_dict_known = {"N.":"Nikolai",
                     "Kt.":"Katarina",
                     "M.":"Maria",
                     "Kh.":"Kungsholms",
                     "Kl.":"Klara",
                     "J.":"Jakobs o. Johannes",
                     "A.":"Adolf Fredriks",
                     "H.":"Hedvig Eleonora",
                     "E.":"Engelbrekts",
                     "O.":"Oscars",
                     "G.":"Gustaf Wasa",
                     "Dj:holm":"Djursholm",
                     "Mt.":"Matteus"}
##################################################

#Define the main step A algorithm and apply it to the dataframe
def perf_match(row):
    if isinstance(row["line"], str):
        tokens = row["line"].split(",") + row["line"].split() + row["line"].split(".")
        line = row["line"]
        for name in df_death_reg_unacc["last_name"].dropna().values:
            if name in tokens and line.startswith(name) and len(name) > 2:
                row["best_match"] = name
                row["last_name"] = name
                row["similarity"] = 100
                row["index"] = "A2"
                row["matched"] = True
                break
    return row



def fuzzy_alt(row, min_score=85, mid_score=90):
        line = row["line"]
        cut = line[:2] if isinstance(line, str) else ""
        pairings = df_death_reg_unacc[df_death_reg_unacc["last_name"].notna()]
        pairings = pairings[pairings["last_name"].str.startswith(cut)]
        pairings = pairings.sort_values(by="last_name", key=lambda x: x.str.len(), ascending=False)

        best_score = 0
        best_name = None

        for last_name in pairings["last_name"]:
            if len(last_name) > len(line):
                continue
            compare_part = line[:len(last_name)]
            score = fuzz.token_sort_ratio(last_name, compare_part)
            if (score > best_score):
                best_score = score
                best_name = last_name
                if (best_score > mid_score and ((abs(min(
                    (line.find(" ", line.find(compare_part) + len(compare_part)) - (line.find(compare_part) + len(compare_part)))
                    if " " in line[line.find(compare_part) + len(compare_part):] else float('inf'),
                    (line.find(",", line.find(compare_part) + len(compare_part)) - (line.find(compare_part) + len(compare_part)))
                    if "," in line[line.find(compare_part) + len(compare_part):] else float('inf'),
                    (line.find(",", line.find(compare_part) + len(compare_part)) - (line.find(compare_part) + len(compare_part)))
                    if "." in line[line.find(compare_part) + len(compare_part):] else float('inf'))) == 0))):
                    row["last_name"] = complete_first_word(line[:len(best_name)],line).rstrip('., ').strip()
                    break
            
        if best_score >= mid_score:
            row["matched"] = True
            row["index"] = "A3"
            row["best_match"] = best_name
            row["similarity"] = best_score
            row["last_name"] = complete_first_word(line[:len(best_name)],line).rstrip('., ').strip()
        else:
            best_score = 0
            best_name = None
            for last_name in df_death_reg_unacc["last_name"].dropna().sort_values(key=lambda x: x.str.len(),ascending=False):
                if len(last_name) > len(line):
                    continue
                compare_part = line[:len(last_name)]
                score = fuzz.token_sort_ratio(last_name, compare_part)
                if score > best_score:
                    best_score = score
                    best_name = last_name
                    if (best_score > mid_score and ((abs(min(
                        (line.find(" ", line.find(compare_part) + len(compare_part)) - (line.find(compare_part) + len(compare_part)))
                        if " " in line[line.find(compare_part) + len(compare_part):] else float('inf'),
                        (line.find(",", line.find(compare_part) + len(compare_part)) - (line.find(compare_part) + len(compare_part)))
                        if "," in line[line.find(compare_part) + len(compare_part):] else float('inf'),
                        (line.find(".", line.find(compare_part) + len(compare_part)) - (line.find(compare_part) + len(compare_part)))
                        if "." in line[line.find(compare_part) + len(compare_part):] else float('inf'))) == 0))):
                        row["last_name"] = complete_first_word(line[:len(best_name)],line).rstrip('., ').strip()
                        break

            complete_word = complete_first_word(line[:len(last_name)], line)

            if best_score >= min_score and abs(len(complete_word) - len(best_name)) <= 5:
                row["matched"] = True
                row["index"] = "A4"
                row["best_match"] = best_name
                row["similarity"] = best_score
                row["last_name"] = complete_first_word(line[:len(best_name)],line).rstrip('., ').strip()
            else:
                row["matched"] = False
                row["index"] = "A5"
                row["best_match"] = ""
                row["similarity"] = 0
                row["last_name"] = ""
        if row["index"] in ["A3", "A4"]:
            try:
                line = row.get("line", "")
                best_name = row.get("best_match", "")
        
                partial = line[:len(best_name)] if best_name else ""
                completed = complete_first_word(partial, line) or ""
        
                remaining = line[line.find(partial) + len(partial):] if partial in line else ""
        
                # Calculate the distance of the various characters
                comma_dist = remaining.find(",") if "," in remaining else float('inf')
                space_dist = remaining.find(" ") if " " in remaining else float('inf')
                dot_dist = remaining.find(".") if "." in remaining else float('inf')
        
                comma_ok = min(comma_dist,space_dist, dot_dist) == 1
        
                comp_name = len(completed) > len(best_name)
        
                if comp_name and not comma_ok:
                    row["matched"] = False
                    row["index"] = "A5"
                    row["best_match"] = ""
                    row["similarity"] = 0
                    row["last_name"] = ""
            except Exception as e:
                row["matched"] = False
                row["index"] = "A5"
                row["best_match"] = ""
                row["similarity"] = 0
                row["last_name"] = ""
        if row["index"] in ["A3", "A4"]:
            line = row.get("line", "")
            if re.search(pattern, line):
                row["matched"] = False
                row["index"] = "A5"
                row["best_match"] = ""
                row["similarity"] = 0
                row["last_name"] = ""
        if row["index"] == "A5":
            for dirty, clean in dirty_last_names_list.itertuples(index=False):
                if dirty in row["line"]:
                    row["matched"]     = True
                    row["index"]       = "A2"
                    row["best_match"]  = clean
                    row["similarity"]  = 100
                    row["last_name"]   = clean
                    break
        return row


def alt_algorithm(row_):
    # A1
    if any(row_["line"].startswith(word) for word in no_occ_list):
        row_["matched"] = True
        row_["index"] = "A1"
        return row_

    # A2: perfect match
    if not row_["matched"]:
        row_ = perf_match(row_)

    # A3 / A4: fuzzy match
    if not row_["matched"]:
        row_ = fuzzy_alt(row_)

    return row_


################################# Activate these 2 only if necessary ################################# 
surname_list = surname_list.apply(alt_algorithm, axis = 1)


# surname_list =  pd.read_pickle("1912_new.pkl")
surname_list["unique_key"] = surname_list["page"].astype(str) + "_" + surname_list["column"].astype(str) + "_" + surname_list["row"].astype(str) + "_" + surname_list["line"].astype(str)

df_dash = surname_list[surname_list["line"] == "-"]
surname_list = surname_list[surname_list["line"] != "-"]

surname_list.to_csv("alt_alg_dpsk_whole.csv")

surname_list = pd.read_csv("alt_alg_dpsk_whole.csv")


#Perform fuzzy match on V. and on the last names w/ dash 
def fuzzy_v_dot_and_dash_LN(row, df=surname_list, min_score=86, mid_score=90):
    line = row["line"]
    line_split = line.split(",")
    last_name = line_split[0]

    # Case "V."
    if line.startswith("V.")  and row["last_name"] == "":
        line = line.replace("V.", "von")
        ln = line.split(",")[0]
        df_unacc_von = df_death_reg_unacc[df_death_reg_unacc["last_name"].str.startswith("von")]
        df_unacc_von = df_unacc_von[abs(df_unacc_von["last_name"].str.len() - len(ln)) <= 1]
        best_fit, score, index__ = fuzzy_match_rapidfuzz(ln, df_unacc_von["last_name"])
        if min_score <= score:
            row["best_match"] = best_fit
            ln = ln.replace("von","V.")
            row["last_name"] = ln
            row["similarity"] = score
            if score == 100:
                row["index"] = "A2"
                row["fuzzy_v_dash"] = 1
            if mid_score <= score < 100:
                row["index"] = "A3"
                row["fuzzy_v_dash"] = 1
            if min_score <= score < mid_score:
                row["index"] = "A4" 
                row["fuzzy_v_dash"] = 1
        row["matched"] = True
        return row

    # Case LN with dash "-"
    if (re.search(r'\w+\s*-\s*\w+', last_name) 
        and not re.search(r'\d+', last_name) 
        and re.search(r'\w+', last_name) 
        and row["last_name"] == "" 
        and not df["line"].duplicated(keep=False).loc[row.name]   # ⬅️ evita duplicati
        and len(line_split) > 1 
        and len(last_name.split()) == 1
       ):
        last_name_splitted = last_name.split("-")
        for comp_ in last_name_splitted:
            death_reg_comp_ = df_death_reg_unacc[abs(df_death_reg_unacc["last_name"].str.len() - len(comp_)) <= 1]
            best_fit, score, index__ = fuzzy_match_rapidfuzz(comp_, death_reg_comp_["last_name"])
            
            if min_score <= score:
                row["best_match"] = str(row["best_match"]) + ' ' + best_fit  
                row["last_name"] = str(row["last_name"] ) + " " + comp_
                row["similarity"] = score
                if score == 100:
                    row["index"] = "A2"
                    row["fuzzy_v_dash"] = 1
                if mid_score <= score < 100:
                    row["index"] = "A3"
                    row["fuzzy_v_dash"] = 1
                if min_score <= score < mid_score:
                    row["index"] = "A4" 
                    row["fuzzy_v_dash"] = 1
            row["matched"] = True
            row["best_match"] = row["best_match"].strip()
            row["last_name"] =  row["last_name"].strip()
        if row["last_name"] != "":
            row["last_name"] = '-'.join(last_name_splitted)
        return row
        
    return row 

surname_list["fuzzy_v_dash"] = 0
surname_list["line_complete"] = surname_list["line"].fillna("")
#surname_list = surname_list.apply(fuzzy_v_dot_and_dash_LN, axis = 1)


#Clear numbers from the extra commas, substitute the 0 with a O when it is an initial and clean double dots
def clean_comma_num(row):
    line = str(row["line_complete"])
    if (re.search(r'\d,\d\d', line) and (not any(x > 100 for x in [int(n) for n in re.findall(r'\d+', line)]) 
                                         or any(x < 10 for x in [int(n) for n in re.findall(r'\d+', line)]))
        ):
        row["line"] = re.sub(r'(\d)\s*,\s*(\d\d)', r'\1\2', row["line"])
    return row
def clean_dot_num(row):
    line = str(row["line_complete"])
    if (re.search(r'\d,\d\d', line) and (not any(x > 100 for x in [int(n) for n in re.findall(r'\d+', line)]) 
                                         or any(x < 10 for x in [int(n) for n in re.findall(r'\d+', line)]))
        ):
        row["line"] = re.sub(r'(\d)\s*,\s*(\d\d)', r'\1\2', row["line"])
    return row
surname_list = surname_list.apply(clean_dot_num, axis=1)



for col in ["line","line_complete"]:
    surname_list[col] = surname_list[col].apply(lambda x: re.sub(r"\s0\.,", " O.,", x))
    surname_list[col] = surname_list.apply(lambda x: re.sub(r"\.\.\s(\d)", r"., \1", x[col]), axis = 1)
    surname_list[col] = surname_list.apply(lambda x: re.sub(r"\.\.\s-", r"., -", x[col]), axis = 1)
    surname_list[col] = surname_list[col].apply(lambda x: re.sub(r'([A-Z])\.\.\s([a-z])', r'\1., \2', str(x)))
    


i = 0

while i < 2:

    #Get the residual line excluding the last name
    def get_the_residual_line(row):
        last_name = row["last_name"]
        residual_line = row["line_complete"] 
    
        if isinstance(last_name, str) and last_name.strip() != "":
            residual_line = residual_line.replace(last_name, "", 1)
    
            # Cut until the first alphabetical character
            for i, ch in enumerate(residual_line):
                if not ch.isalpha():
                    residual_line = residual_line[i:]
                else:
                    break
    
            residual_line = residual_line.strip()
    
        # assegna il risultato alla colonna del df
        row["residual_line"] = residual_line
        return row
    
    
    surname_list["residual_line"] = ""
    surname_list = surname_list.apply(get_the_residual_line, axis = 1)
    
    #Get initials
    initials_pattern = r'\b(?:[A-Z]{1,3}\.|[A-Z]:\w+|[A-Z]:\s|[A-Z]:,|[A-Z][a-z]\.|[A-Z][a-z]{2}\.|\. [A-Z]\.)'
    
    def get_initials(row):
        line = str(row["residual_line"])  # sicurezza
        line = re.sub(r"\s*,\s*", ", ", line)
        line_split = line.split()
    
        initials = []
        start_ = False
        
        
        for i, token in enumerate(line_split):
            if re.search(initials_pattern, token):
                initials.append(token)
                start_ = True
    
                # controlla se bisogna fermarsi
                next_token = line_split[i + 1] if i + 1 < len(line_split) else ""
                if "," in token or (next_token and len(next_token) > 4 and re.search(r'[a-z]', next_token)):
                    break
            elif start_:
                break
    
        row["initials"] = ' '.join(initials).replace(",", "")
        if re.search(r'A\s*\.?\s*-\s*B',row["initials"]):
            row["initials"] = re.sub(r'A\s*\.?\s*-\s*B\.?', "", row["initials"])
            
        return row
    
    surname_list["initials"] = ""
    surname_list = surname_list.apply(get_initials, axis = 1)
    
    #Step Get the first name of the group
    def build_prefix_dict(first_names, prefix_len=2):
        prefix_dict = defaultdict(list)
        for name in first_names:
            prefix_dict[name[:prefix_len]].append(name)
        return prefix_dict
    
    prefix_dict = build_prefix_dict(first_names)
    
    def first_name(df):
        idx_list = df.index.to_list()
        for pos, idx in enumerate(idx_list):
            initials = df.at[idx, "initials"]
            line = df.at[idx, "line"].replace(",", "")
            last_name = df.at[idx, "last_name"]
    
            if initials == "" and not re.search(pattern, line):
                for word in line.split()[:-1]:
                    candidates = prefix_dict.get(word[:2], []) 
                    if word in candidates and word != last_name:
                        df.at[idx, "initials"] = word
                        break
        return df
    surname_list = first_name(surname_list)
    #surname_list = surname_list.apply(get_initials, axis = 1)
    
    #Update the residual lines
    def update_the_residual_line(row):
        initials = row["initials"]
        residual_line = row["residual_line"]
    
        if isinstance(initials, str) and initials.strip() != "":
            residual_line = residual_line.replace(initials, "", 1)
    
            # Cut until the first alphabetical character
            for i, ch in enumerate(residual_line):
                if not ch.isalpha():
                    residual_line = residual_line[i:]
                else:
                    break
    
            residual_line = residual_line.strip()
    
        # assegna il risultato alla colonna del df
        row["residual_line"] = residual_line
        if initials == ".":
            row["initials"] = ""
        if initials and initials[0] == ".":
            row["initials"] = initials[0:].strip() 
        return row
    
    surname_list = surname_list.apply(update_the_residual_line, axis = 1)
    
    
    #Get the second last name
    surname_list["second_last_name"] = ""
    def second_last_name(row):
        remaining_line = row["residual_line"]
        remaining_line_split = remaining_line.split()
    
        
        if remaining_line_split:
            first_token = remaining_line_split[0].replace(",", "")
            # regex: inizia con Maiuscola, poi opzionale minuscola, poi ":", poi 1-2 lettere minuscole
            if ":" in first_token and re.search(r'[A-Z]', first_token) and not re.search(pattern, first_token):
                row["second_last_name"] = first_token
        
        return row
    
    def update_the_residual_line(row):
        second_last_name = row["second_last_name"]
        residual_line = row["residual_line"]
    
        if isinstance(second_last_name, str) and second_last_name.strip() != "":
            residual_line = residual_line.replace(second_last_name, "", 1)
    
            for i, ch in enumerate(residual_line):
                if not ch.isalpha():
                    residual_line = residual_line[i:]
                else:
                    break
    
            residual_line = residual_line.strip()
    
        row["residual_line"] = residual_line
        return row
    
    surname_list = surname_list.apply(second_last_name, axis = 1)
    surname_list = surname_list.apply(update_the_residual_line, axis = 1)
    
    
    #Occupation part
    occ_list = pd.read_csv("occ_list_for_alg.csv",index_col=0)
    occ_list = occ_list[["occ_llm"]]
    occ_list["occ_llm"] = remove_accents(occ_list["occ_llm"]) 
    
    occ_list = occ_list.sort_values(by="occ_llm", key=lambda x: x.str.len(), ascending=False)
    #Get rid of f.d.
    surname_list["f_d_"] = surname_list["residual_line"].apply(lambda x: 1 if re.search(r'\bf\.\s*d\.', x) else 0) 
    surname_list["residual_line"] = surname_list.apply(
        lambda x: re.sub(r'\bf\.\s*d\.', "", str(x["residual_line"])) if x["f_d_"] == 1 else x["residual_line"],
        axis=1)
    surname_list["residual_line"] = surname_list.apply(lambda x: x["residual_line"][0:].strip() if x["f_d_"] == 1 and  x["residual_line"][0] == ","
                                                       else x["residual_line"], axis = 1)
    
    def extract_occ(row):
        line = row["residual_line"]
        if isinstance(line, str):
            line = re.sub(r'\s+', ' ', line.strip().lower())
            for word in occ_list["occ_llm"]:
                #Check if the precise word is in the line
                def clean_token(s):
                    return s.strip().lower().translate(str.maketrans('', '', string.punctuation))
                
                if isinstance(word, str) and word.lower().rstrip() in line: ####################Previous version
                    if isinstance(word, str) and (
                        any(clean_token(word) == clean_token(lw) for lw in line.split()) or
                        any(clean_token(word) == clean_token(lw) for lw in line.split(","))):
                    
                        row["occ_reg"] = word
                        break
        row["occ_reg"] = "" if len(row["occ_reg"]) < 3  or row["occ_reg"] == "hustru" else row["occ_reg"].strip()
        row["occ_reg"] = row["occ_reg"].lower()
        return row 
        

                
    surname_list["occ_reg"] = "" 
    surname_list = surname_list.apply(extract_occ, axis = 1)

    
    
    def update_the_residual_line(row):
        if row["occ_reg"] != "":
            occ_reg = row["occ_reg"].strip() 
        else:
            if row["residual_line"].split(",")[0].strip().islower():
                occ_reg = row["residual_line"].split(",")[0].strip()
            else:
                return row
    
        
        row["residual_line"] = row["residual_line"].strip()
        residual_line = row["residual_line"]
    
        if isinstance(occ_reg, str) and occ_reg.strip() != "" and occ_reg.lower() in residual_line.lower():
            index_fin = residual_line.lower().index(occ_reg.lower()) + len(occ_reg)
            residual_line = residual_line[index_fin:]
    
            for i, ch in enumerate(residual_line):
                if not ch.isalpha():
                    residual_line = residual_line[i:]
                else:
                    break
    
            residual_line = residual_line.strip()
    
        row["residual_line"] = residual_line
        return row
    
    surname_list = surname_list.apply(update_the_residual_line, axis = 1)
    
    
    
    
    
    ##########################################
    #Create the function that splits the lines
    ##########################################
    
    def split_line(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()            # SettingWithCopyWarning
        df["split"] = 0           # Start
    
        idx_list = df.index.to_list()   # Real index
    
        for pos, idx in enumerate(idx_list):
            line = str(df.at[idx, "line"]) if pd.notna(df.at[idx, "line"]) else ""
            line = line.rstrip()
            
            #Automatically assign 3 if the index is A1
            if df.at[idx, "index"] == "A1" and re.search(r'\d+', line):
                df.at[idx, "split"] = 3
                continue
    
            # Jump if the split has been already been assigned
            if df.at[idx, "split"] != 0:
                continue
            
            #Cover a specific case
            if df.at[idx, "line"].startswith("Paul U. Bergstroms A.-B.") and df.at[idx, "row"] == 1:
                continue
            
            
            
            # ---------- Condition 1 ----------
            # has letters + ( "-num" )
            if (re.search(r'[A-Z]', line) and re.search(r'[a-z]', line) and any(n>1000 for n in [int(x) for x in re.findall(r'\d+', line)]) and
                len(re.findall(r'\d', line)) > 1  and re.search(r'-\s*\d+', line) and len(line) > 5 and#Numbers condition
                len(max(max(line.split(), key=len, default=""), max(line.split(","), key=len, default=""), key=len)) > 4):
                if pos + 1 < len(idx_list):
                    nxt = idx_list[pos + 1]
                    next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                    combined = line + next_line
                if ( not re.search(r'[A-Za-z]', next_line) and len(re.findall(r'\d+', next_line))==1 and all(n>1000 for n in [int(x) for x in re.findall(r'\d+', next_line)]) and
                    (re.search(r'\d+\s*-\s*\d+\s*-\s*\d+', combined) or re.search(r'\d+\s*-\s*\d+\s\d+', combined) and all(n>15000 for n in [int(x) for x in re.findall(r'\d+', next_line)]))
                    ):
                    df.at[idx, "split"] = 1
                    df.at[nxt, "split"] = 2
                else:
                    df.at[idx, "split"] = 3
                continue
            
            
    
            # ---------- Condition 1-bis ----------
            # line has a letter
            # line must contain a number or an initial or a firm pattern
            # line does not start with a number
            # line ends with '-' or ',' or a letter or with a number and has either an initial or a firm pattern
            # Checks if the next line starts with a capital letter
            if (line and re.search(r'[A-Z]', line) and re.search(r'[a-z]', line)  and
                re.search(r'\d+', line) and len(re.findall(r'\d', line)) > 1 #Numbers condition
                and (df.at[idx,"initials"] != "" #Firm or initial condition #MODIFIED: orig: and (re.search(r'\b(?:[A-Z]{1,3}\.|[A-Z]:\w+)', line)
                     or re.search(pattern, line)) #Firm or initial condition #MODIFIED
                and not line[0].isdigit() and (line.endswith('-') or line[-1].isalpha() or line.endswith(',') or
                                               ((df.at[idx,"initials"] != "" #MODIFIED: Original: re.search(initials_pattern, line)
                                                or re.search(pattern, line))
                                                and (line[-1].isdigit()))  )):
                if pos + 1 < len(idx_list):
                    nxt = idx_list[pos + 1]
                    next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                    if next_line and (next_line[0].isupper() or (next_line.startswith("de ") and any(n>1000 for n in [int(x) for x in re.findall(r'\d+', line)])) ):
                        df.at[idx, "split"] = 3
                        continue
            
            #A few adjustments to manage some specific cases
            #if line and line[-1] in [">","<"]:
             #   line.replace(">", "")
              #  line.replace("<", "")
            if line and line.endswith(("->", "-.", ">", "<","/")):
                line = line[:-1]
                
            # ---------- Condition 2 ----------
            # Candidate split (ends with number / letter / '-' / ',')
            # + has letters + evaluation of concat with next line
            #   pattern "- num" without the following line starting with a capital letter
            if (line and not line[0].isnumeric() and (re.search(r'[A-Za-z]', line.split(",")[0]) or re.search(r'[A-Za-z]', line.split()[0]))
                and (line[-1].isdigit() or line[-1] in ['-', '.',',', ')',";",">"] or line[-1].isalpha())
                and re.search(r'[A-Z]', line) and re.search(r'[a-z]', line) and len(line) > 10 and not re.search(r'\d+\s*inv\.\)', line) #Before it was 8
                and pos + 1 < len(idx_list)
            ):
                nxt = idx_list[pos + 1]
                next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                combined = line + next_line
                if (df.at[nxt,"index"] != "A1" and (re.search(r'-\s*\d+', combined) or re.search(r'\s*\d+-', combined) or re.search(r'\b\d+\s+\d+\b', next_line) or (re.search(r'\d+', next_line) and all(n>1000 for n in [int(x) for x in re.findall(r'\d+', next_line)])) ) and 
                         (((not next_line or (not next_line[0].isupper() or (next_line[0].isupper() and next_line[1] in [".",","]) or (next_line[0].isupper() and next_line[2] in [".",","]))) and
                    any(num > 1000 for num in [int(x) for x in re.findall(r'\d+', next_line)])) or  #Extra line here 
                          
                                                      
                     (re.search(pattern, line)     and any(num > 1000 for num in [int(x) for x in re.findall(r'\d+', next_line)]) and
                     (not next_line or (not next_line[0].isupper() or (next_line[0].isupper() and not next_line.startswith("A.-B") and next_line[1] in [".",","]) or 
                                                                              (next_line[0].isupper() and next_line[2] in [".",","])))))
    
                    ):
                    df.at[idx, "split"]     = 1
                    df.at[nxt, "split"]     = 2
                    continue
            #Extra control: ----------------- Condition 2 - bis  ---------------- Case without "-"
            
            def only_firm_occup_pattern_hyphens(line: str) -> bool: #Create an ad hoc condition for the "without -" pattern check 
                #firm_pattern = [r'A.-B.',"A-B","A- B","-B.","-B\.",'c:o',"Svenska","-akt.-bol.","svenska","forening","sthlms","-akt","akt.-","-b.","societ","aktie","Aktie","-bol","bol.",
                 #               "bolag","Bostad","bank","L:td","a.-b.","akt.-bol.",'fonden']
                firm_pattern = [re.sub(r'\\', '', p) for p in pattern.split('|')] + ["fabrik", "sverk","Bank", "bank ", "bank,","Jarnvag ","Jarnvag,","jarnvag,","jarnvag " ]
                firm_pattern = [w for w in firm_pattern if "-" in w]
                occ_with_line = occ_list[occ_list["occ_llm"].str.contains("-")]
                occ_with_line  = occ_with_line.sort_values(by="occ_llm", key=lambda x: x.str.len(), ascending=False)
                
                # Find all lines in the string
                hyphen_positions = [m.start() for m in re.finditer(r'-', line)]
                
                # If there are no lines
                if not hyphen_positions:
                    return True
                
                
                #If there are lines in the firm pattern
                for word in firm_pattern:
                    if word in line:
                        control_ = line.replace(word, "")
                        if "-" in control_:
                            return False
                        else:
                            return True
                #If there are lines in the occupation pattern
                for word in occ_with_line["occ_llm"]:
                    if word in line:
                        control_ = line.replace(word, "")
                        if "-" in control_:
                            return False
                        else:
                            return True
                if re.search(r'\b[a-zA-Z]+(\s*-\s*)[a-zA-Z]+\b', line): #Accept the "-" only if it is between two letters
                    return True
                
            
            if (line and (re.search(r'[A-Za-z]', line.split(",")[0]) or re.search(r'[A-Za-z]', line.split()[0])) and only_firm_occup_pattern_hyphens(line) and len(line) > 10 and not re.search(r'\d+(?:\s+|-)\d+', line) and not re.search(r'\d+\s*inv\.\)', line) and #Before the length lim was 5 
                  ((re.search(r'[a-z]', line) and re.search(r'[A-Z]', line) and line[0].isupper()) or 
                    re.search(pattern, line)  )   
                  ):
    
                    if pos + 1 < len(idx_list):
                        nxt = idx_list[pos + 1]
                        next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                        
                        nums = re.findall(r'\d+', next_line)
                        if df.at[nxt,"index"] != "A1" and  nums and "-" not in next_line and not re.search(r'[A-Za-z]', next_line):
                            nums_int = [int(n) for n in nums]  
                        
                            if (len(nums_int) == 1 and nums_int[0] > 1000) or (len(nums_int) == 2 and min(nums_int) > 1000) :
                                next_line = "-" + next_line
                                combined = line + next_line
                
                                if (
                                    (re.search(r'-\s*\d+', combined) and not next_line[0].isupper() )
                                    or
                                    ((re.search(r'\s*\d+-', combined) or re.search(r'^-\s*\d+\s+\d+$', next_line)) and
                                     re.search(pattern, line) and
                                     (not next_line[0].isupper() or
                                      (next_line[0].isupper() and not next_line.startswith("A.-B") and next_line[1] in [".", ","]) or
                                      (next_line[0].isupper() and next_line[2] in [".", ","])))
                                ):
                                    df.at[idx, "split"] = 1
                                    df.at[nxt, "split"] = 2
                                    continue
            #Extra control -------------- Condition 2 extra ---------- Case split plus "inc-"
            if (line and not re.search(r'\d', line) and len(line) > 10 and not re.search(r'\d+\s*inv\.\)', line) and ((line[0].isupper() and re.search(r'[a-z]', line) and re.search(r'[A-Z]', line) and "," in line and 
                                                                             df.at[idx, "initials"] != "") or 
                                                         re.search(pattern, line))
                ): 
                if pos + 1 < len(idx_list):
                    nxt = idx_list[pos + 1]
                    next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                    nums_ = re.findall(r'\d+\s*-', next_line)
                    nums = re.findall(r'\d+(?=\s*-)', next_line)
                    if df.at[nxt,"index"] != "A1" and nums_ and nums and len(nums) == 1 and int(nums[0]) > 1000 and not re.search(r'[A-Za-z]', next_line):
                        combined_line = line + next_line
                        if re.search(r'\d+\s*-', combined_line):   
                            df.at[idx, "split"] = 1
                            df.at[nxt, "split"] = 2
                            continue
            
                        
        return df
            
        
    
    
    ######################################
    #Adjust for a line split in 3 sections
    ######################################
    def third_line(df):
        idx_list = df.index.to_list()
        occ__ = occ_list[occ_list["occ_llm"].str.len() > 3]
        for pos, idx in enumerate(idx_list):
            line = df.at[idx, "line"]
            
            # Check: split = 2 and length of the line
            if df.at[idx, "split"] == 2 and (len(line) > 25 or ( not re.search(r'[A-Za-z]',line) and not re.search(r'[A-Za-z]',line))):
                
                # Verifica che ci sia una riga successiva
                if pos + 1 < len(idx_list):
                    idx_next = idx_list[pos + 1]
                    next_line = df.at[idx_next, "line"]
                    
                    # Previous line check
                    if pos - 1 >= 0:
                        idx_prev = idx_list[pos - 1]
                        prev_compl_line = df.at[idx_prev, "line_complete"]
                        prev_line = df.at[idx_prev, "line"]
                        combined_ = prev_compl_line + next_line
    
                        # Conditons for the third line
                        if (all(n > 6000 for n in [int(x) for x in re.findall(r'\d+', next_line)]) and next_line != "-" and
                            (all(n > 1000 for n in [int(x) for x in re.findall(r'\d+', line)]) or len(line) > 35) and
                            not re.search(r'[A-Za-z]', next_line) and 
                            df.at[idx_next, "split"] == 0 and len(re.findall(r'\d+', combined_)) <= 3 and not any(word in df.at[idx, "line"] for word in occ__) and
                            (re.search(pattern, prev_line) or df.at[idx_prev,"initials"] != "") and not any(w in re.findall(pattern, line) for w in re.findall(pattern, prev_line)) 
                            ):
                            
                            df.at[idx_next, "split"] = 4
                            df.at[idx_prev, "line_complete"] = prev_compl_line + " " + next_line
    
        return df
    
    
    
    
    ###############
    #Extract income
    ###############
    
    #Create a function that unites separated lines
    def unite_lines(df_):
        df_["line_complete"] = df_["line"].fillna("")  # riempi i NaN con stringa vuota
        for i in range(len(df_) - 1):
            if df_.iloc[i]["split"] == 1:
                current = str(df_.iloc[i]["line"]).rstrip()
                if str(df_.iloc[i + 1]["line"]) != "-":
                    next_line = str(df_.iloc[i + 1]["line"]).lstrip()
                else:
                    next_line = str(df_.iloc[i + 2]["line"]).lstrip()
                df_.at[df_.index[i], "line_complete"] = current + " " + next_line
        return df_
    
    
    #Function that extracts income from the complete lines
    def extr_inc(row):
        def income_until_punct(s: str) -> str | None:
            i = len(s) - 1
            income_ = []
            started = False
            while i >= 0:
                ch = s[i]
                
                if ch.isdigit():
                    income_.insert(0, ch)
                    started = True
                elif ch in [',', '.'] and i != len(s) - 1:
                    # Stop the loop if there is a dot or a comma which is not at the end of the line
                    break
                elif not ch.isalpha() and started:
                    income_.insert(0, ch)
                elif ch.isalpha():
                    break  # Stop the loop if there is a letter
                i -= 1
            
            return ''.join(income_) if income_ else None
    
        if row.get("split") in [0,1, 3] and isinstance(row.get("line_complete"), str):
            row["income"] = income_until_punct(row["line_complete"])
        else:
            row["income"] = None
        row["income"] = row["income"].lstrip() if isinstance(row["income"], str) else row["income"]
        return row 
    
    
    #Function that splits the two incomes 
    def split_income(row):
        income = row.get("income")
        if not isinstance(income, str):
            income = "" 
        
        income_1 = ""
        income_2 = ""
        buffer = []
        
        start = False
        first_end = False
        i = 0
        
        while i < len(income):
            ch = income[i]
            
            if ch.isdigit():
                buffer.append(ch)
                start = True
                i += 1
            else:
                if start:
                    if not first_end:
                        income_1 = ''.join(buffer)
                        buffer = []
                        first_end = True
                        start = False
                    else:
                        income_2 = ''.join(buffer)
                        break
                i += 1
        
        if not first_end and buffer:
            income_1 = ''.join(buffer)
        elif first_end and buffer:
            income_2 = ''.join(buffer)
        
        row["income_1"] = income_1
        row["income_2"] = income_2
        return row
    
    
    #Apply all the functions created before at once
    def find_income(df_):
        if df_ is not None and not df_.empty:
            df_ = unite_lines(df_)
            df_= third_line(df_)
            df_["income"] = 0  # optional
            df_ = df_.apply(extr_inc, axis=1)
            df_["income_1"] = ""  
            df_["income_2"] = ""  
            df_ = df_.apply(split_income, axis = 1)
            df_["income"] = df_["income"].apply(lambda x: x if bool(re.search(r'\d', str(x))) else "") #Make empty strings if there is no income
    
        return df_
    
    if i == 0:
        surname_list = surname_list.groupby(["page","column"]).apply(split_line)
        surname_list = surname_list.drop(columns = {"column","page"}).reset_index()
        surname_list = surname_list.drop(columns = {"level_2"})
        surname_list = find_income(surname_list) #Unite the lines
    
    #Last shots for the loop
    if i == 1:
        surname_list["income"] = 0  # optional
        surname_list = surname_list.apply(extr_inc, axis=1)
        surname_list["income_1"] = ""  
        surname_list["income_2"] = ""  
        surname_list = surname_list.apply(split_income, axis = 1)
        surname_list["income"] = surname_list["income"].apply(lambda x: x if bool(re.search(r'\d', str(x))) else "") #Make empty strings if there is no income
        
        #Adjust second lines
        def adj_sec_lowercase_LN(df):
            idx_list = df.index.to_list()
            for pos, idx in enumerate(idx_list):
                line = df.at[idx,"line"]
                prv_line = None   # Initialization sempre
                nxt_line = None
                last_name = df.at[idx,"last_name"]
                if pos - 1 >= 0:
                    prv = idx_list[pos - 1]
                    prv_line = df.at[prv,"line"]
                if pos + 1 < len(df):
                    nxt = idx_list[pos + 1]
                    next_line = df.at[nxt,"line"]
                if (prv_line is not None and prv_line[-1] != "-" and df.at[idx, "split"] == 2 and line.startswith(last_name) and not re.search("\(", line) and len(last_name) > 1 and
                    (last_name[0].islower() or (last_name[0].isupper() and last_name[1] == "."))
                    ):
                    df.at[idx,"split"] = 0
                    df.at[prv,"split"] = 0
                    if not re.search(r'[A-Za-z]',next_line) and any(x>1000 for x in [int(n) for n in re.findall(r'\d+', next_line )]) and df.at[nxt, "split"] == 0:
                        df.at[idx,"split"] = 1
                        df.at[nxt, "split"] = 2
                        df.at[idx,"line_complete"] = df.at[idx,"line_complete"] + df.at[nxt,"line_complete"]
                    else:
                        df.at[idx,"split"] = 3
                        
                    if (len(prv_line.split(",")) > 2 and (re.search(pattern, prv_line) or re.search(initials_pattern, prv_line)) 
                        and any(x>1000 for x in [int(n) for n in re.findall(r'\d+', next_line )])):
                        df.at[prv,"split"] = 3
            return df
        surname_list = adj_sec_lowercase_LN(surname_list)
        
        
        
                
        def extract_parish(row):
            parish_pattern = r'\b(?:[A-Z]{1,3}\.?|[A-Z]:\w+|[A-Z]:\s|[A-Z]:,|[A-Z][a-z]\.?|[A-Z][a-z]{2}\.?)'
            line = row["line_complete"]
        
            # preparo versioni "pulite"
            line_no_comma = line.replace(",", " ").replace("-", "")
            line_no_comma = re.sub(r'\s+', ' ', line_no_comma).strip()
            line_no_comma = re.sub(r'(\d+)[A-Za-z]+', r'\1', line_no_comma)
            line_no_comma_split = line_no_comma.split()
        
        
            # cerco il primo token numerico
            inter_ = [x for x in line_no_comma_split if not re.search(r'[A-Za-z]', x) and re.search(r'\d', x)]
            if not inter_:
                return row  
        
            # prendo la parola prima del numero
            pos_inc = line_no_comma_split.index(inter_[0])
            if pos_inc == 0:
                return row
            
            found_ = False
            candidate = ""
            j = 1
            while found_ == False and pos_inc - j > 0: #Update
                string_ = line_no_comma_split[pos_inc - j]
                if re.search(r'[A-Za-z]', string_):
                    candidate = string_
                    break
                else:
                    j += 1
        
            # controllo che sia un candidato parish
            if candidate and ((re.search(initials_pattern, candidate) or re.search(parish_pattern, candidate) or re.fullmatch(r"[A-Z][a-z]", candidate)) and row["split"] in [1,3]):
                pos_cand = line.find(candidate)
                if pos_cand == -1:   # se non trovato, salto
                    return row
        
                # torno indietro fino alla virgola
                i = pos_cand - 1
                while i >= 0:
                    if line[i] == ",":
                        break
                    i -= 1
        
                # salvo il candidato
                row["parish"] = candidate
        
                # funzione helper: controlla se c'è virgola tra due iniziali
                def comma_betw_init(s, line_2=line):
                    tokens = s.split()
                    for j in range(len(tokens) - 1):
                        w, next_init = tokens[j], tokens[j+1]
                        try:
                            index_1 = line_2.index(w)
                            index_2 = line_2.index(next_init)
                        except ValueError:
                            continue
                        if "," in line_2[index_1:index_2]:
                            return True
                    return False
        
                # prendo sempre initials come stringa
                initials_str = str(row["initials"])
        
                # se candidate coincide con una initial → la rimuovo
                if (candidate in initials_str) and comma_betw_init(initials_str):
                    if initials_str.split()[-1] == candidate:
                        row["initials"] = " ".join(initials_str.split()[:-1])
                    else:
                        row["initials"] = initials_str.replace(candidate, "")
        
            return row
        
        
        
        # applicazione
        surname_list["parish"] = ""
        surname_list = surname_list.apply(extract_parish, axis=1)
        
        
        def extract_parish_no_init(row):
            line = row["line_complete"]
            line_sec = line#.replace("\d+-", "\d+")
            line_sec = re.sub(r'\s+', ' ', line_sec).strip()
            line_sec = re.sub(r'(\d+)[A-Za-z]+', r'\1', line_sec)
            line_split = line_sec.split(",")
            for h, token in enumerate(line_split):
                if re.search(r'\d+', token):  
                    token_clean = token.replace("-", " ")
                    parts = token_clean.split()
                    if len(parts) > 1:
                        line_split = line_split[:h] + parts + line_split[h+1:]
                    
            
            inter_ = [x for x in line_split if not re.search(r'[A-Za-z]', x) and re.search(r'\d', x)]
            if not inter_:
                return row  
        
            # prendo la parola prima del numero
            pos_inc = line_split.index(inter_[0])
            if pos_inc == 0:
                return row
            
            found_ = False
            candidate = ""
            j = 1
            while found_ == False and pos_inc - j > 0: #Update:
                string_ = line_split[pos_inc - j]
                if re.search(r'[A-Za-z]', string_):
                    candidate = string_
                    break
                else:
                    j += 1
            
            candidate = candidate.strip()
            
            if ((re.search("-", candidate) or re.search(":", candidate) or any(word.islower() for word in candidate.split())) and row["split"] in [1,3] and row["parish"] == "" 
                and any(re.search(r'[a-z]', word) for word in candidate.split("-")) and any(re.search(r'[A-Z]', word) for word in candidate.split("-"))):
                pos_cand = line.find(candidate)
                if pos_cand == -1 and re.search(r'\d+', candidate):   # se non trovato, salto
                    return row
        
                # torno indietro fino alla virgola
                i = pos_cand - 1
                while i >= 0:
                    if line[i] == ",":
                        break
                    i -= 1
        
                # salvo il candidato
                if candidate != row["occ_reg"]:
                    row["parish"] = candidate
            return row
        
        surname_list = surname_list.apply(extract_parish_no_init, axis=1)
        
        
        
        
        def extra_parish_residual_cases(row):
            line = row["line_complete"]
            line_sec = line#.replace("\d+-", "\d+")
            line_sec = re.sub(r'\s+', ' ', line_sec).strip()
            line_sec = re.sub(r'(\d+)[A-Za-z]+', r'\1', line_sec)
            line_split = line_sec.split(",")
            for h, token in enumerate(line_split):
                if re.search(r'\d+\s*-\s*\d+', token):  
                    token_clean = token.replace("-", " ")
                    parts = token_clean.split()
                    if len(parts) > 1:
                        line_split = line_split[:h] + parts + line_split[h+1:]
                    
            
            inter_ = [x for x in line_split if not re.search(r'[A-Za-z]', x) and re.search(r'\d', x)]
            if not inter_:
                return row  
        
            # prendo la parola prima del numero
            pos_inc = line_split.index(inter_[0])
            if pos_inc == 0:
                return row
            
            found_ = False
            candidate = ""
            j = 1
            while found_ == False and pos_inc - j > 0: #Update:
                string_ = line_split[pos_inc - j]
                if re.search(r'[A-Za-z]', string_):
                    candidate = string_
                    break
                else:
                    j += 1
            
            candidate = candidate.strip()
            
            if (((re.search("-", candidate) or re.search(":", candidate) or any(word.islower() for word in candidate.split())) and row["split"] in [1,3] and row["parish"] == "" 
                and any(re.search(r'[a-z]', word) for word in candidate.split("-")) and any(re.search(r'[A-Z]', word) for word in candidate.split("-"))) or
                re.fullmatch(r"[a-z]\.?", candidate) or re.fullmatch(r"[A-Z]][a-z]\.?", candidate)):
                pos_cand = line.find(candidate)
                if pos_cand == -1 and re.search(r'\d+', candidate):   # se non trovato, salto
                    return row
        
                # torno indietro fino alla virgola
                i = pos_cand - 1
                while i >= 0:
                    if line[i] == ",":
                        break
                    i -= 1
        
                # salvo il candidato
                if candidate != row["occ_reg"]:
                    row["parish"] = candidate
            return row
        
        surname_list = surname_list.apply(extra_parish_residual_cases, axis=1)
        
        def spot_wrong_occ(row):
            occ_reg = str(row["occ_reg"]).strip()
            parish = str(row["parish"]).strip()
            
            # Se parish e occ_reg coincidono
            if parish and occ_reg and parish == occ_reg:
                # E non è un'occupazione valida (ignora maiuscole/minuscole)
                if not any(occ_reg.lower() == str(word).lower().strip() for word in occ_list["occ_llm"]):
                    row["change_occ"] = 1
                    row["occ_reg"] = ""
                else:
                    row["change_occ"] = 1
                    row["parish"] = ""
            return row
        
        surname_list["change_occ"] = 0
        surname_list = surname_list.apply(spot_wrong_occ, axis=1)
        
        surname_list["parish"] = surname_list.apply(lambda x: "" if x["parish"] == x["parish"] and x["index"] == "A1" else x["parish"], axis = 1)
        
        
        def adj_initials_dupl(row):
            initials = str(row["initials"])
            line = str(row["line_complete"])
        
            # cerca duplicati nelle initials
            for w in initials.split():
                if initials.split().count(w) > 1:  
                    # trova tutte le posizioni di w in line
                    positions = [m.start() for m in re.finditer(w, line)]
                    
                    if len(positions) > 1:
                        # prendi il testo compreso tra la prima e l’ultima occorrenza
                        line_cut = line[positions[0]:positions[-1]]
        
                        # se c'è una virgola dentro → taglio le initials prima dell’ultima occorrenza
                        if "," in line_cut:
                            last_occ = initials.rfind(w)
                            row["initials"] = initials[:last_occ].strip()
            return row
        
        surname_list = surname_list.apply(adj_initials_dupl, axis=1)


        #Extra adjustments to parishes
        surname_list["parish"] = surname_list["parish"].apply(lambda x: re.sub(r'\d+', "", str(x)))
        surname_list["parish"] = surname_list["parish"].apply(lambda x: "" if re.search(pattern, x) else x)
        surname_list["parish"] = surname_list.apply(lambda x: "" if x["parish"].endswith(x["occ_reg"]) else x["parish"], axis = 1)
        #surname_list["parish"] = surname_list.apply(lambda x: "" if len(str(x["parish"])) > 25 else x["parish"], axis = 1)
        surname_list["parish"] = surname_list.apply( lambda row: "" 
            if (
                row["parish"].lower() in occ_list["occ_llm"].values or re.search(pattern, row["parish"])
                or any(word in occ_list["occ_llm"].values for word in row["parish"].lower().split())
            ) 
            and len(re.findall(r'[a-z]', row["parish"])) > 4   and "-" not in row["parish"] else row["parish"],
            axis=1
        )
                
        
        
        
        #Firm and estate tokens
        #Firm token/dummy REFINED: adjusted w/ the brackets part
        def firm_token(row):
            line = row["line_complete"]
            if pd.notna(line) and re.search(pattern, line):
                row["firm_dummy"] = 1
            if ("(" in line 
                ): #New part
                new_complete_line = re.sub(r'\([^)]*\)', '', line)
                if not re.search(pattern, new_complete_line):
                    row["firm_dummy"] = 0
            if ")" in line and "(" not in line:
                def strip_parentheses_fragments(s: str) -> str:
                    s2 = re.sub(r'\([^)]*\)', '', s)
                
                    # 2. Se resta una parentesi chiusa senza aperta → tronco tutto a sinistra
                    if ")" in s2 and "(" not in s2:
                        # prendi solo quello che c’è dopo l’ultima parentesi chiusa
                        s2 = s2.split(")")[-1].strip()
                
                    # 3. Se resta una parentesi aperta senza chiusa → tronco tutto a destra
                    if "(" in s2 and ")" not in s2:
                        # prendi solo quello che c’è prima della parentesi aperta
                        s2 = s2.split("(")[0].strip()
                
                    return s2
                new_complete_line_2 = strip_parentheses_fragments(line)
                if not re.search(pattern, new_complete_line_2):
                    row["firm_dummy"] = 0

            return row
        surname_list["firm_dummy"] = 0
        surname_list = surname_list.apply(firm_token, axis=1)


        #Adjust for lines which have both firm dummy and  occ_reg
        def _ind_FT(df):
            df["change"] = 0
            df_copy = df[
                (df["occ_reg"] != "") &
                (df["firm_dummy"] == 1) &
                (df["last_name"] != "") &
                (df["line"].str.contains(r'[A-Z]\w*,\s*\b(?:[A-Z]\.)'))
            ]

            df_copy["line"] = df_copy["line"].str.replace("A.-B.", "", regex=False)
            #df_copy["line_complete"] = df_copy["line_complete"].str.replace("A.-B.", "", regex=False)

            # Nuovo filtro dopo la sostituzione
            df_copy = df_copy[
                (df_copy["occ_reg"] != "") &
                (df_copy["firm_dummy"] == 1) &
                (df_copy["last_name"] != "") &
                (df_copy["line"].str.contains(r'\w+,\s*\b(?:[A-Z]\.)')) 
                #& (~df_copy["line_complete"].str.contains("c:o"))
            ]

            def get_ind_with_FT(row):
                last_name = row["last_name"]
                line = row["line"]
                line_complete = row["line_complete"]
                
                # Look for the firm pattern within the brackets
                if "(" in line_complete:
                    match_original = re.search(pattern, line_complete)
                    new_complete_line = re.sub(r'\([^)]*\)', '', line_complete)
                    match_clean = re.search(pattern, new_complete_line)

                    if match_original and not match_clean:
                        row["firm_dummy"] = 0
                        row["change"] = 1
                    return row
                
                if last_name.endswith("s"):
                    last_name_upd = last_name[:-1]
                    if last_name_upd not in df_death_reg_unacc["last_name"]:
                        row["firm_dummy"] = 0
                        row["change"] = 1
                        return row

                if re.search(r'dir|kontorist', line):
                    row["firm_dummy"] = 0
                    row["change"] = 1
                    return row

                if line.startswith("Bank"):
                    row["firm_dummy"] = 0
                    row["change"] = 1
                    return row
                

                line_low = line_complete.lower()
                start_pos_occ = line_low.find(row["occ_reg"])
                if start_pos_occ != -1:
                    end_pos_occ = start_pos_occ + len(row["occ_reg"]) - 1
                    list_next_word = [line_low[end_pos_occ + 1:x] for x in range(end_pos_occ + 2, len(line_complete))]
                    if any(re.search(pattern, x) for x in list_next_word):
                        row["firm_dummy"] = 0
                        row["change"] = 1
                    return row

                return row

            df_copy = df_copy.apply(get_ind_with_FT, axis=1)

            # Update only the firm dummy
            df.loc[df_copy.index, ["firm_dummy","change"]] = df_copy[["firm_dummy", "change"]]
            
            #Update if we have a firm dummy 
            idx_list_ = df.index.to_list()
            
            for pos, idx in enumerate(idx_list_[:-1]):  # esclude l'ultimo per evitare out of range
                nxt = idx_list_[pos + 1]
            
                if (
                    df.at[idx, "split"] == 1
                    and df.at[idx, "change"] == 1
                    and df.at[nxt, "split"] == 2
                    and df.at[nxt, "firm_dummy"] == 1
                ):
                    df.at[nxt, "firm_dummy"] = 0

            return df


        surname_list = _ind_FT(surname_list)
        
        
        
        #Adjust firm dummies 
        surname_list["initials"] = surname_list.apply(
            lambda row: "" if (
                row["firm_dummy"] == 1
                and isinstance(row["initials"], str) 
                and len(re.findall(r'[a-z]', row["initials"])) > 3) else row["initials"],
            axis=1)


        #Get the estate token
        estate_pattern = r'st\.-hus|starbh|sterbh|starkbhus|starb-|starb\'h|sta bh'
        def estate_token(row):
            line = row["line_complete"]
            if pd.notna(line) and re.search(estate_pattern, line) and not re.search(r'starbhusnot\.', line):
                row["estate_dummy"] = 1
            return row
        surname_list["estate_dummy"] = 0
        surname_list = surname_list.apply(estate_token, axis=1)


        
    
        
    i += 1



#Create the location list and assigne each locations 
###############################################################
def find_locations(row):
    line = row["line"]
    def extr_until_brackets(s):
        s_fin = []
        for i in range(0,len(s)):
            if not s[i] in ["(",")"]:
                s_fin.append(s[i])
            else:
                break
        return ''.join(s_fin)
    if re.search("inv\.\)",line):
        row["location"] = extr_until_brackets(row["line"])
        row["location"] = re.sub(r'\d+', "", row["location"])
        row["location"] = re.sub(r'inv\.', "", row["location"])
        row["location"] = re.sub(r',', "", row["location"])
    return row
surname_list["location"] = ""
surname_list = surname_list.apply(find_locations, axis = 1)

#Create a clean list of locations
location_list = surname_list[surname_list["location"] != ""]
location_list = location_list[["page","column","row","line","line_complete","split","location"]]
location_list["location"] = location_list.apply(lambda x:surname_list.loc[(surname_list["page"] == x["page"]) & (surname_list["column"] == x["column"]) & (surname_list["row"] == int(x["row"])-1),"line"].values[0] if x["location"] == " " else x["location"], axis = 1)
location_list = pd.concat([pd.DataFrame({"page": [0],"column": [0],"row": [0],"line": ["Stockholm"],"line_complete": ["Stockholm"],"split": [0],"location": ["Stockholm"]}),location_list], axis = 0)

#Assign the location
def extract_location(df):
    df["municipality"] = ""

    # Set a starting value
    i = 0
    index_list_ = df.index.to_list()
    start_value = location_list.iloc[0]["location"]

    for pos, idx in enumerate(index_list_):
        page = int(df.at[idx, "page"])
        row = int(df.at[idx, "row"])

        # Caso 1: prima delle location note
        if page < location_list["page"].min():
            df.at[idx, "municipality"] = start_value
            continue

        # Caso 2: pagina non presente in location_list
        if page not in location_list["page"].values:
            df.at[idx, "municipality"] = location_list.iloc[i]["location"]
            continue

        # Caso 3: pagina trovata in location_list
        municipalities = location_list[location_list["page"] == page]
        n_ = len(municipalities)

        # Controllo se passo alla prossima pagina
        if pos + 1 < len(df):
            nxt = index_list_[pos + 1]
            if df.at[idx, "page"] != df.at[nxt, "page"]:
                i += n_

        # Se la riga è sopra la prima location → assegno direttamente
        if row < municipalities["row"].min():
            df.at[idx, "municipality"] = location_list.iloc[i]["location"]
            continue

        # Se la riga è dopo → prendo la location precedente
        if row > municipalities["row"].min():
            iter_ = pd.concat([df.loc[[idx]], municipalities], axis=0)
            iter_ = iter_.sort_values(by="row").reset_index(drop=True)  # indice univoco
        
            # Trovo la posizione del record corrente in iter_
            pos_2 = iter_[iter_["row"] == row].index[0]  # adesso index è sicuro univoco
        
            prev_row = iter_.iloc[pos_2 - 1] if pos_2 > 0 else None
            if prev_row is not None:
                df.at[idx, "municipality"] = prev_row["location"]
            continue


    return df

def location_limit_case(df):
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        line = df.at[idx,"line"]
        prv = idx_list[pos - 1]
        if pos + 3 < len(df):
            nxt = idx_list[pos + 3]
            next_mun = df.at[nxt,"municipality"]
        prev_mun = df.at[prv,"municipality"]
        if df.at[idx,"location"] == "" and df.at[idx,"municipality"] == "":
            if re.search(r'[A-G]',line[0]):
                df.at[idx,"municipality"] = next_mun
                continue
            if re.search(r'[O-Z]', line[0]):
                df.at[idx,"municipality"] = prev_mun
    return df 

surname_list = extract_location(surname_list)
surname_list = location_limit_case(surname_list)
###############################################################
def adj_suspect_occ(row):
    line = row["line_complete"]
    if row["occ_reg"] == "" and row["firm_dummy"] == 0 and row["split"] in [1,3] and row["index"] != "A1" and not re.search(r'froken|ankefru|\bfru',line):
        line_split = line.split(",")
        line_split = [x.strip() for x in line_split]
        for word in line_split:
            if any(word == occ_ for occ_ in occ_list["occ_llm"].values):
                row["occ_reg"] = word       
    return row
surname_list = surname_list.apply(adj_suspect_occ, axis = 1)



###############################################################



surname_list.to_csv("a_4.csv")

surname_list = pd.read_csv("a_4.csv")

cols = ["second_last_name", "occ_reg","income","income_1","income_2", "last_name","best_match","initials"]

for col in cols:
    surname_list[col] = surname_list[col].apply(lambda x: "" if pd.isna(x) else x)


#Adjust extra lines first half 
def adj_extra_FH(df):
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        line = df.at[idx,"line"]
        if pos + 2 < len(df):
            nxt = idx_list[pos + 1]
            nxt_nxt = idx_list[pos + 2]
            if df.at[idx,"split"] == 1 and df.at[nxt,"split"] != 2 and df.at[nxt,"line"] != "-" and df.at[nxt_nxt,"split"] != 2:
                if (df.at[idx,"initials"] != "" and re.search(r'\d+', line)) or df.at[idx,"occ_reg"] != "" :
                    df.at[idx,"split"] = 3
                else:
                    df.at[idx,"split"] = 0
    return df
surname_list = adj_extra_FH(surname_list)



#Find_out the pages to cut
pages_to_cut = surname_list.groupby("page").filter( lambda g: (((g["last_name"].str.strip() != "").sum() < 5) and ((g["occ_reg"].str.strip() != "").sum() < 5) 
                                                               and ((g["firm_dummy"] != 0).sum() < 10) ) or 
                                                   ((g["line"].str.len()>60).sum() > 3) or ((g["occ_reg"].str.strip() != "").sum() < 1))  #Captures the long lines and few occupations

pages_to_cut = pages_to_cut["page"].unique()



i = 0
certain = {}

while i < 2:
    def certain_lines(df):
    
        #df = df.copy()
        df["pages_to_cut"] = 0
        df["certain_estate"] = 0
        df["certain_locations_inv"] = 0
        df["certain_Tel_int"] = 0
        df["only_dash"] = 0
        df["certain_ind_A1_complete"] = 0
        df["IT_1"] = 0
        df["IT_2"] = 0
        df["certain_firms"] = 0
        df["certain_noise_A"] = 0
        df["certain_noise_B"] = 0
    
        # Elimination of the first certain group 
        df["pages_to_cut"] = df["page"].apply(lambda x: 1 if x in pages_to_cut else 0)
        
        
        df["certain_locations_inv"] = df.apply(lambda x: 1 if (pd.notna(x["line"]) and re.search(r'inv\.\)', str(x["line"])) and
                                               x["pages_to_cut"] == 0) else 0, axis = 1)
    
        df["certain_Tel_int"] = df.apply(
            lambda x: 1 if (
                pd.notna(x["line_complete"]) and (
                    re.search(r'[Tt]el\.\s*\d', x["line_complete"]) or
                    re.search(r'[Tt]el\s*\d', x["line_complete"]) or
                    re.search(r'[Tt]el\.\s', x["line_complete"]) or re.search(r'Allm\.\s*[Tt]el', x["line_complete"]) 
                ) 
            and x["pages_to_cut"] == 0#Added now
            ) else 0,
            axis=1
        )
    
        #df["only_dash"] = df["line"].apply(lambda x: 1 if str(x) == "-" else 0)
        df["only_dash"] = df.apply(lambda x: 1 if (not re.search(r'[A-Za-z]',str(x["line"])) and 
                                           not re.search(r'\d+', str(x["line"])) and x["pages_to_cut"] == 0) else 0, axis = 1)
    
    
        certain = {
            "certain_only_dash": df[df["only_dash"] == 1],
            "certain_locations_inv": df[df["certain_locations_inv"] == 1],
            "certain_Tel_int": df[df["certain_Tel_int"] == 1],
            "pages_to_cut": df[df["pages_to_cut"] == 1]
        }
    
        df_clean = df[(df["only_dash"] == 0) & (df["certain_locations_inv"] == 0) & (df["certain_Tel_int"] == 0)].copy()
        idx_list_ = df_clean.index.to_list()
    
        for pos, idx in enumerate(idx_list_):
            line_compact = "".join(str(v) if pd.notna(v) else "" 
                                   for v in [df_clean.at[idx, "last_name"], df_clean.at[idx, "initials"], df_clean.at[idx, "occ_reg"]])
            line_compact_strip = re.sub(rf"[{string.punctuation}\s]+", "", line_compact)
            
            #Consider the second option
            initials_2 = df_clean.at[idx, "initials"].split()[:-1]
            initials_2_str = "".join(initials_2) 
            #initials_2_str = re.sub(rf"[{string.punctuation}\s]+", "", initials_2_str)
            line_compact_2 = "".join(str(v) if pd.notna(v) else "" 
                                   for v in [df_clean.at[idx, "last_name"], initials_2_str, df_clean.at[idx, "occ_reg"]])
            line_compact_strip_2 = re.sub(rf"[{string.punctuation}\s]+", "", line_compact_2)
    
            line_complete = str(df_clean.at[idx, "line_complete"]) if pd.notna(df_clean.at[idx, "line_complete"]) else ""
            line_complete_strip = re.sub(rf"[{string.punctuation}\s]+", "", line_complete)
    
            line = str(df_clean.at[idx, "line"]) if pd.notna(df_clean.at[idx, "line"]) else ""
            
            
         
            
            
            
            if (df_clean.at[idx,"pages_to_cut"] != 0 or df_clean.at[idx,"certain_estate"]!=0 or df_clean.at[idx,"certain_locations_inv"] != 0 or df_clean.at[idx,"certain_Tel_int"] != 0 or
                    df_clean.at[idx,"only_dash"] != 0 or df_clean.at[idx,"certain_ind_A1_complete"]!= 0 or  df_clean.at[idx,"IT_1"] != 0 or 
                    df_clean.at[idx,"IT_2"] != 0 or    df_clean.at[idx,"certain_firms"] != 0 or    df_clean.at[idx,"certain_noise_A"] != 0 or
                    df_clean.at[idx,"certain_noise_B"] != 0):
                continue
            
            
    
            # Take the next line
            next_line = ""
            if pos + 1 < len(df_clean):
                nxt = idx_list_[pos + 1]
                next_line = str(df_clean.at[nxt, "line"]) if pd.notna(df_clean.at[nxt, "line"]) else ""
    
    
    
    
    
            #Estate tokens
            if df_clean.at[idx,"estate_dummy"] == 1:
                df_clean.at[idx, "certain_estate"] = 1
                if df_clean.at[idx,"split"] == 1 and pos + 1 < len(df_clean) and df_clean.at[nxt, "split"] == 2:
                    df_clean.at[nxt,"certain_estate"] = 1
                continue
                    
    
    
    
            # A1
            if df_clean.at[idx, "index"] == "A1":
                df_clean.at[idx, "certain_ind_A1_complete"] = 1
                continue
    
    
            #LN plus initials plus occupation
            if (
                df_clean.at[idx, "split"] and df_clean.at[idx, "occ_reg"] != "" and df_clean.at[idx, "last_name"] != "" and df_clean.at[idx, "initials"] != "" and
                (line_complete_strip.lower().startswith(line_compact_strip.lower()) or 
                 (line_complete_strip.lower().startswith(line_compact_strip_2.lower()) and df.at[idx,"initials"] != "") ) 
                and df_clean.at[idx, "split"] != 2 and df_clean.at[idx, "firm_dummy"] == 0 and
                ((df_clean.at[nxt, "firm_dummy"] == 0 and df_clean.at[idx, "split"]==1) or df_clean.at[idx, "split"] in [0,2,3])
            ):
                df_clean.at[idx, "IT_1"] = 1
                if df_clean.at[idx, "split"] == 1 and pos + 1 < len(df_clean) and df_clean.at[nxt, "split"] == 2:
                    df_clean.at[nxt, "IT_1"] = 1
                continue
    
    
            #IT 2
            if (
                df_clean.at[idx, "firm_dummy"] == 0 and df_clean.at[idx, "split"] != 2 
                and
                (df_clean.at[idx, "occ_reg"] != "" or (df_clean.at[idx, "initials"] != "" and re.search(r'\d+', line_complete))) and
                not (df_clean.at[idx,"split"] == 0 and df_clean.at[idx,"index"] == "A5")
                ):
                df_clean.at[idx, "IT_2"] = 1
                if df_clean.at[idx, "split"] == 1 and pos + 1 < len(df_clean) and df_clean.at[nxt, "split"] == 2:
                    df_clean.at[nxt, "IT_2"] = 1
                continue
    
            #Certain firms
            if (
                df_clean.at[idx, "firm_dummy"] == 1 and not (df_clean.at[idx, "occ_reg"] != "" and
                df_clean.at[idx, "initials"] != "" and df_clean.at[idx, "last_name"] != "")
            ):
                df_clean.at[idx, "certain_firms"] = 1
                if df_clean.at[idx, "split"] == 1 and pos + 1 < len(df_clean) and df_clean.at[nxt, "split"] == 2:
                    df_clean.at[nxt, "certain_firms"] = 1
                continue
    
            #Certain noises
            if (
                df_clean.at[idx, "index"] == "A5" and df_clean.at[idx, "split"] == 0 and df_clean.at[idx, "firm_dummy"] == 0 
                and df_clean["line"].duplicated(keep=False).loc[idx] #Add the duplicates in CN
            ):
                first_word = line.split()[0] if line else ""
                first_word_comma = line.split(",")[0] if "," in line else ""
                if not (first_word in cities_par or first_word_comma in cities_par):
                    if len(re.findall(r'\w+', line)) in [1, 2] and re.search(r'\d+', line):
                        df_clean.at[idx, "certain_noise_B"] = 1
                    else:
                        df_clean.at[idx, "certain_noise_A"] = 1
                continue
    
        # Update certain dictionary
        
        #certain["pages_to_cut"] = df_clean[df_clean["pages_to_cut"] == 1]
        
        certain["certain_estate_complete"] = df_clean[(df_clean["certain_estate"] == 1) & (df_clean["split"] == 3)]
        certain["certain_estate_FH_SH"] = df_clean[(df_clean["certain_estate"] == 1) & (df_clean["split"].isin([1,2]))]
        certain["certain_estate_noise"] = df_clean[(df_clean["certain_estate"] == 1) & (df_clean["split"] == 0)]

        
        certain["df_A1"] = df_clean[df_clean["certain_ind_A1_complete"] == 1]
        
        certain["df_IT_1_complete"] = df_clean[(df_clean["IT_1"] == 1) & (df_clean["split"] == 3)]
        certain["df_IT_1_noise"] = df_clean[(df_clean["IT_1"] == 1) & (df_clean["split"] == 0)]
        certain["df_IT_1_FH_SH"] = df_clean[(df_clean["IT_1"] == 1) & (df_clean["split"].isin([1, 2]))]
            
    
        certain["df_IT_2_complete"] = df_clean[(df_clean["IT_2"] == 1) & (df_clean["split"] == 3)]
        certain["df_IT_2_noise"] = df_clean[(df_clean["IT_2"] == 1) & (df_clean["split"] == 0)]
        certain["df_IT_2_FH_SH"] = df_clean[(df_clean["IT_2"] == 1) & (df_clean["split"].isin([1, 2]))]
    
        certain["df_CF_complete"] = df_clean[(df_clean["certain_firms"] == 1) & (df_clean["split"] == 3)]
        certain["df_CF_noise"] = df_clean[(df_clean["certain_firms"] == 1) & (df_clean["split"] == 0)]
        certain["df_CF_FH_SH"] = df_clean[(df_clean["certain_firms"] == 1) & (df_clean["split"].isin([1, 2]))]
    
        certain["df_certain_noise_A"] = df_clean[df_clean["certain_noise_A"] == 1]
        certain["df_certain_noise_B"] = df_clean[df_clean["certain_noise_B"] == 1]
    
        return df, certain
    
    surname_list, certain = certain_lines(surname_list)
    
    
    
    
    #Potential second lines section
    def potential_sec_lines(df, certain):
        for col in ["potential_sec_line_A", "potential_sec_line_B","potential_sec_line_C","potential_sec_line_D"]:
            #if col not in df.columns:
            df[col] = 0
    
        all_certain_lines = pd.concat(
            [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
            ignore_index=True
        )
        df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()
        
        #Number 16: Potential second line 1 remaining lines w/ only integers
        def first_pot_sec_line(row): 
            if not re.search(r'[A-Za-z]', row["line"]) and row["split"] in [0,2]: #Before there was no 2
                row["potential_sec_line_A"] = 1
            return row
        df_filt = df_filt.apply(first_pot_sec_line, axis=1)
        df.update(df_filt)
        certain["potential_sec_line_A"] = df[df["potential_sec_line_A"] == 1]
    
    
        
        
        #Number 17: Potential second line 2 line w/ occupation and integer
    
        all_certain_lines = pd.concat(
            [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
            ignore_index=True
        )
        df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()
        def sec_pot_sec_line(row):
            occ_ = row["occ_reg"]
            line__ = row["line"]
            if row["split"] in [0,2] and occ_ != "" and re.search(r'\d+',line__): #Before, the split was only 0 
                row["potential_sec_line_B"] = 1
            return row
        df_filt = df_filt.apply(sec_pot_sec_line, axis=1)
        df.update(df_filt)
        certain["potential_sec_line_B"] = df[df["potential_sec_line_B"] == 1]
    
    
    
        
        #Number 18: Potential second line 3 line w/ municipality and integer
        all_certain_lines = pd.concat(
            [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
            ignore_index=True
        )
        df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()
        def third_pot_sec_line(row):
            line__ = row["line"]
            if (row["split"] in [0,2,3] #Before there was no 2
                and row[ "firm_dummy"] == 0 and not re.search(r'inv\.\)', line__) and (line__.split()[0] in cities_par or line__.split(",")[0] in cities_par) and
                re.search(r'\d+', line__) and any(int(n) > 1000 for n in re.findall(r'\d+', line__)) and not re.search(r'inv\.\)', line__)  
                ):
                row["potential_sec_line_C"] = 1
            return row
        df_filt = df_filt.apply(third_pot_sec_line, axis=1)
        df.update(df_filt)
        certain["potential_sec_line_C"] = df[df["potential_sec_line_C"] == 1]
        
        
        
        #Number 19 potential second line 4: line w/ letters and integers > 1000
        all_certain_lines = pd.concat(
            [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
            ignore_index=True
        )
        df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()
        def fourth_pot_sec_line(row):
            line__ = row["line"]
            if (row["split"] in [0,2,3] # Before there was no 2
                and re.search(r'[A-Za-z]', line__) and any(n > 1000 for n in [int(x) for x in re.findall(r'\d+', line__)])):
                row["potential_sec_line_D"] = 1
            return row
        df_filt = df_filt.apply(fourth_pot_sec_line, axis=1)
        df.update(df_filt)
        certain["potential_sec_line_D"] = df[df["potential_sec_line_D"] == 1]    
        
        return df, certain
    surname_list, certain = potential_sec_lines(surname_list, certain)
    
    
    #This code reassignes potential second lines 
    def adj_sec_lines(df):
        potential_sec_lines_df = pd.concat([
            certain["potential_sec_line_C"],
            certain["potential_sec_line_A"],
            certain["potential_sec_line_B"],
            certain["potential_sec_line_D"]
            ],axis=0)
        idx_list_ = df.index.to_list()
    
        for pos, idx in enumerate(idx_list_):

            if df.at[idx,"unique_key"] in potential_sec_lines_df["unique_key"].values:
                prv = idx_list_[pos - 1]
                line_prev = df.at[prv, "line"]
                split_prec = df.at[prv, "split"]
                line = df.at[idx, "line"]
                split_act = df.at[idx, "split"]            
                if ( split_act != 2 ######I do not want to touch assigned second lines
                    and (split_prec == 0 or (split_prec == 3 and not re.search(r'[A-Za-z]', line)))  and
                    (line_prev[0].isupper() or line_prev.startswith("von ") or line_prev.startswith("de ") or line_prev.startswith("af. ") or line_prev.startswith("af ")) and 
                    (re.search(initials_pattern, line_prev) or df.at[prv,"firm_dummy"] == 1 or df.at[prv,"initials"] != "") and #Are we sure we need this??????????????????????????
                    len(line_prev) > 10 and re.search(r'[a-z]', line_prev) 
                    and 
                    df.at[prv,"unique_key"] not in certain["certain_Tel_int"]["unique_key"].values):    
                    df.at[prv, "split"] = 1
                    df.at[idx, "split"] = 2
        return df
    
    if i == 0:
        surname_list = adj_sec_lines(surname_list)
    
    remaining_lines = surname_list[~surname_list["unique_key"].isin(pd.concat([df["unique_key"] for df in certain.values()]))].sort_values(by="index")
    
    

    #Find potential first lines    
    def potential_FH(df):  
        df = df.sort_values(by=["page", "column", "row"])
        index_list_ = df.index.to_list()
        df["pot_first_line"] = 0
    
        for pos, idx in enumerate(index_list_):
            line = str(df.at[idx, "line"])
            split = df.at[idx, "split"]
    
            next_line_series = surname_list.loc[
                (surname_list["page"] == df.at[idx, "page"]) &
                (surname_list["column"] == df.at[idx, "column"]) &
                (surname_list["row"] == int(df.at[idx, "row"]) + 1),
                "line"
            ]
    
            if next_line_series.empty:
                continue
    
            next_line = str(next_line_series.iloc[0])
    
            if (
                (line[0].isupper() or re.match(r"(von\s|de\s)", line)) and re.search(r'[A-Za-z]',line) and 
                (re.search(initials_pattern, line) or re.search(pattern, line)) and
                any(int(n) > 1000 for n in re.findall(r'\d+', next_line)) and
                line[0:2] != next_line[0:2] and
                split not in [2] and len(line) > 8 #Before there was also the 1
            ):
                df.at[idx, "pot_first_line"] = 1
    
        certain["pot_first_line"] = df[df["pot_first_line"] == 1]
        df = df[df["pot_first_line"] == 0]
        return df
    
    remaining_lines = potential_FH(remaining_lines)
    
    #Aadjust them and bring them back
    def _adj_pot_FH(df):
        df["pot_first_line"] = df["unique_key"].isin(
            certain["pot_first_line"]["unique_key"]).astype(int)
        index_list_ = df.index.to_list()
        for pos, idx in enumerate(index_list_):
            line = df.at[idx,"line"]
            split = df.at[idx,"split"]
            if pos + 1 < len(df):
                nxt = index_list_[pos + 1]
                next_line = df.at[nxt,"line"]
            if df.at[idx,"pot_first_line"] == 1 and df.at[idx,"split"] not in [1]: #Before there was not this part
                if (df.at[nxt,"split"] not in [1,2] and re.search(r'\d+', next_line) and
                    line[0:2] != next_line[0:2] and any(n > 1000 for n in [int(x) for x in 
                                                                           re.findall(r'\d+',next_line)])
                    and ((split == 3 and not re.search(r'[A-Za-z]',next_line)) or split == 0)
                    and df.at[nxt,"index"] != "A1" ):
                    df.at[idx,"split"] = 1
                    df.at[nxt,"split"] = 2
        return df
    
    if i == 0:
        surname_list = _adj_pot_FH(surname_list)
        surname_list = find_income(surname_list)
        
        #########################################################
        #Replicate initials and parishes ONLY EXTRA PART ADDED
        #surname_list["initials"] = ""
        #surname_list = surname_list.apply(get_initials, axis = 1)
        #surname_list = first_name(surname_list)
        #surname_list = surname_list.fillna("")
        #surname_list = surname_list.apply(second_last_name, axis = 1)

        surname_list["parish"] = ""
        surname_list = surname_list.apply(extract_parish, axis=1)        
        surname_list = surname_list.apply(extract_parish_no_init, axis=1)        
        surname_list = surname_list.apply(extra_parish_residual_cases, axis=1)
        
        surname_list["change_occ"] = 0
        surname_list = surname_list.apply(spot_wrong_occ, axis=1)
        
        surname_list["parish"] = surname_list.apply(lambda x: "" if x["parish"] == x["parish"] and x["index"] == "A1" else x["parish"], axis = 1)        
        surname_list = surname_list.apply(adj_initials_dupl, axis=1)


        #Extra adjustments to parishes
        surname_list["parish"] = surname_list["parish"].apply(lambda x: re.sub(r'\d+', "", str(x)))
        surname_list["parish"] = surname_list["parish"].apply(lambda x: "" if re.search(pattern, x) else x)
        surname_list["parish"] = surname_list.apply(lambda x: "" if x["parish"].endswith(x["occ_reg"]) else x["parish"], axis = 1)
        surname_list["parish"] = surname_list.apply( lambda row: "" 
            if (
                row["parish"].lower() in occ_list["occ_llm"].values or re.search(pattern, row["parish"])
                or any(word in occ_list["occ_llm"].values for word in row["parish"].lower().split())
            ) 
            and len(re.findall(r'[a-z]', row["parish"])) > 4   and "-" not in row["parish"] else row["parish"],
            axis=1)
        
        
        #Add occupation review
        surname_list = surname_list.apply(adj_suspect_occ, axis = 1)

        
        #################################################################
    if i == 0: #Take out the fake second lines merged to noises
        def take_out_fake_sec_lines(df):
            idx_list = df.index.to_list()
            for pos, idx in enumerate(idx_list):
                split_ = df.at[idx,"split"]
                
                if split_ in [0,1,3]:
                    continue
                
                last_name_ = df.at[idx,"last_name"]
                line = df.at[idx,"line"]
                if (split_ == 2 and df.at[idx,"initials"] != "" and last_name_ and df.at[idx,"occ_reg"] != "" and df.at[idx,"parish"] != "" and df.at[idx,"line"].startswith(last_name_) and
                    (not last_name_[0].islower() or last_name_[0:3] in ["von","af ","de "]) 
                    ):
                    prv = idx_list[pos - 1]
                    df.at[prv,"split"] = 0
                    df.at[idx,"split"] = 0            
                    nxt = idx_list[pos + 1]
                    next_line = df.at[nxt,"line"]
                    if re.search(r'-\s*\d+',line) and re.search(r'[A-Za-z]', next_line):
                        df.at[idx,"split"] = 3
            return df
        #surname_list = take_out_fake_sec_lines(surname_list)
        
    if i == 0:
        #Adjust Allm tel. lines
        def adj_tell_split(row):
            if row["split"] == 3 and row["unique_key"] in certain["certain_Tel_int"]["unique_key"].values:
                row["split"] = 0
            return row
        surname_list = surname_list.apply(adj_tell_split, axis = 1)
        del certain
        
        #Adjust firms + occ_reg + LN + initials
        def adj_ind_plus_FT(df):
            indx_list = df.index.to_list()
            for pos,idx in enumerate(indx_list):
                if pos + 1 < len(df):
                    nxt = indx_list[pos + 1]
                    
                if (df.at[idx,"occ_reg"] != "" and df.at[idx,"initials"] != "" and df.at[idx,"last_name"] != "" 
                    and df.at[idx,"firm_dummy"] == 1):
                    df.at[idx,"firm_dummy"] = 0
                    if df.at[idx,"split"] == 1:
                        split_next,firm_next = surname_list.loc[(surname_list["page"] == df.at[idx,"page"]) &
                                                      (surname_list["column"] == df.at[idx,"column"]) &
                                                      (surname_list["row"] == int(df.at[idx,"page"])+1),
                                                      ["split","firm_dummy"]]
                        if split_next == 2 and firm_next == 1:
                            surname_list.loc[(surname_list["page"] == df.at[idx,"page"]) &
                                                          (surname_list["column"] == df.at[idx,"column"]) &
                                                          (surname_list["row"] == int(df.at[idx,"page"])+1),
                                                          "firm_dummy"] = 0
            return df
        #remaining_lines = adj_ind_plus_FT(remaining_lines)
        del remaining_lines   
    
    
    
    i += 1
    
    
#Brief extra occ_extraction
def occ_fuzz(row):
    line = str(row["line_complete"])  
    if (
        row["split"] in [1, 3]
        and row["firm_dummy"] == 0
        and row["estate_dummy"] == 0
        and row["index"] != "A1"
        and row["occ_reg"] == ""
        and any(word and word[0].islower() for word in line.split())
    ):
        lower_cases_ = " ".join([w for w in line.split() if w and w[0].islower()])
        lower_cases_ = lower_cases_.replace(",", " ").strip()

        if not lower_cases_:
            return ""
        parts = [p.strip() for p in lower_cases_.split(",") if p.strip()]
        candidate = parts[1] if len(parts) > 1 else parts[0]
        candidate = candidate.strip()

        best_match, score, idx_ = fuzzy_match_rapidfuzz(candidate, occ_list["occ_llm"])
        return best_match if score >= 85.5 else ""

    return ""   
surname_list["occ_reg"] = surname_list.apply(lambda row: row["occ_reg"] if row["occ_reg"] != "" else occ_fuzz(row), axis=1)
     
#Extract_extra_occ
def sec_occup(row):
    line = str(row["line_complete"])
    line_2 = str(row["line_complete"])
    ln = str(row["last_name"])
    occ_ = str(row["occ_reg"])
    if row["split"] in [1,3] and row["firm_dummy"] == 0 and row["estate_dummy"] == 0 and row["index"] != "A1" and row["occ_reg"] != "":
        line_split = line.split(",")
        line_split = [w.strip() for w in line_split if w and w.strip() and w.strip()[0].islower()]
        line_split = [x for x in line_split if fuzz.token_sort_ratio(x, occ_) < 82 and x not in ln]
        if not line_split:
            return ""
        parts = [p.strip() for p in line_split if p.strip()]
        candidate = parts[1] if len(parts) > 1 and line.startswith(parts[0]) else parts[0]
        candidate = candidate.strip()

        best_match, score, idx_ = fuzzy_match_rapidfuzz(candidate, occ_list["occ_llm"])
        return candidate if score >= 87 and not occ_ in candidate and not candidate in occ_ and not line.startswith(candidate) and candidate.lower() != occ_.lower() else ""
    
surname_list["occ_reg_2"] = surname_list.apply(lambda row: row["occ_reg"] if row["occ_reg"] == "" else sec_occup(row), axis=1)
    
   
    
    
    
#Final adjustments to parishes:
def fin_adj_par(row):
    initials = row["initials"]
    if row["parish"] == initials and any(initials.split().count(w) == 1 for w in initials.split()) and row["firm_dummy"] == 1 and len(initials.split()) == 1:
        row["initials"] = ""
    return row
surname_list = surname_list.apply(fin_adj_par, axis = 1)


df_subset =  surname_list[(~surname_list["line"].str.contains(pattern, regex=True, na=False))  # gestisce anche NaN in line
                       & (surname_list["last_name"] != "")                              # esclude NaN
                       & (surname_list["split"].isin([1,3]) ) 
                       & (surname_list["parish"] == "") & (surname_list["line"].str.contains(r'[A-Za-z]', regex = True)) &
                       (surname_list["index"] != "A1")]
df_subset = df_subset.apply(extract_parish, axis=1) 
df_subset = df_subset.apply(extract_parish_no_init, axis=1)
df_subset = df_subset.apply(extra_parish_residual_cases, axis=1)
#df_subset = df_subset.apply(general_adj, axis = 1)

surname_list.update(df_subset)


#Get a refined parish name using the information in the book
def cleaned_parish(row, dict_=parish_dict_known):
    parish = str(row["parish"])

    if parish in dict_:
        row["parish_cleaned_"] = dict_[parish]
        return row

    parish_letters = "".join(re.findall(r"[A-Za-z]", parish))
    for key, value in dict_.items():
        key_letters = "".join(re.findall(r"[A-Za-z]", key))
        if abs(len(key) - len(parish)) <= 1 and parish_letters == key_letters:
            row["parish_cleaned_"] = value
            return row
    return row

surname_list["parish_cleaned_"] = ""
surname_list = surname_list.fillna("")
proper_parish = pd.read_csv("proper_parish.csv", index_col=0)
proper_parish = proper_parish.fillna("")
surname_list = surname_list.apply(cleaned_parish, axis = 1)

def parish_map(row):
    parish = row["parish"]
    if row["parish_cleaned_"] != "":
        return row
    row_ = proper_parish.loc[proper_parish["parish"] == parish]
    if not row_.empty:
        mapped = row_["mapped_parish"].values[0]
        cleaned = row_["parish_cleaned"].values[0]
        row["parish_cleaned_"] = mapped if mapped != "" else cleaned
    
    return row

surname_list = surname_list.apply(parish_map, axis=1)



# Merge the parishes
surname_list = surname_list.merge(proper_parish[["parish", "mapped_parish", "parish_cleaned"]],
    on="parish",how="left"
)

surname_list["parish_cleaned_"] = surname_list.apply(lambda row: row["mapped_parish"]
    if pd.notna(row["mapped_parish"]) and row["mapped_parish"] != ""
    else (row["parish_cleaned"] if pd.notna(row["parish_cleaned"]) and row["parish_cleaned"] != "" else row["parish"]),
    axis=1
)
surname_list = surname_list.fillna("")
surname_list = surname_list.apply(cleaned_parish, axis = 1)

surname_list = surname_list.drop(columns = {"parish_cleaned","mapped_parish"})



######################################################################################
#Take parishes for those which do not have them erroneously
def parish_adjustment(row, comma = True):
    line = row["line_complete"]
    if (row["parish"] == "" and row["split"] in [1,3] and row["initials"] != "" 
        and row["index"] != "A1"):
        line_split = line.split(",") if comma else line.split()
        number = re.findall(r'\d+', line) if re.findall(r'\d+', line) else ""
        number = [x for x in number if int(x) > 50]
        number = number[0] if number else ""
        idx = next((i for i, part in enumerate(line_split) if number in part), None)
        if idx > 0: #idx is not None and
            candidate = line_split[idx - 1].strip()
            if candidate == "" and idx - 1 > 0:
                candidate = line_split[idx - 2].strip()
            if candidate in proper_parish["parish"].values or re.search(r'[A-Z]',candidate):
                row["parish"] = candidate
                row_ = proper_parish.loc[proper_parish["parish"] == candidate]
                if not row_.empty:
                    mapped = row_["mapped_parish"].values[0]
                    cleaned = row_["parish_cleaned"].values[0]
                    row["parish_cleaned_"] = mapped if mapped != "" else cleaned 
    return row
surname_list = surname_list.apply(lambda row: parish_adjustment(row, comma=True), axis=1)
surname_list = surname_list.apply(lambda row: parish_adjustment(row, comma=False), axis=1)


#There are some parishes that have extra words because there is no comma
parish_num = pd.DataFrame(surname_list["parish"].unique())
parish_firm = parish_num[parish_num[0].str.contains(pattern, regex = True)] 
parish_num = parish_num[~parish_num[0].isin(parish_firm[0])] #Actual parishes 
def remove_firms_from_parish(row, actual_parish = parish_num):
    parish = row["parish"]
    if any(word == parish for word in parish_firm[0].values) or (row["occ_reg"] in parish and len(row["occ_reg"]) > 4) or len(parish) > 30:
        row["parish"] = ""
        candidate = parish.split()[-1]
        if any(w == candidate for w in actual_parish[0].values) and not re.search(pattern, candidate):
            row["parish"] = candidate
            return row     
    return row
surname_list = surname_list.apply(remove_firms_from_parish, axis=1)

#Adjust some firms
def firms_parishes_(row, initials = True, comma = True):
    line = row["line_complete"]
    if row["firm_dummy"] == 1  and row["parish"] == "" and row["split"] in [1,3]: #and row["municipality"] == "Stockholm"
        line_split = line.split(",") if comma else line.split()
        numbers = re.findall(r'\d+', line)
        numbers = [x for x in numbers if int(x) > 50]
        number = numbers[0] if numbers else ""
        part = [w for w in line_split if number in w]
        part = part[0] if part else ""
        pos = line_split.index(part)
        candidate = line_split[pos - 1]if pos > 0 else ""
        candidate = candidate.strip()
        if initials:
            if (re.search(initials_pattern, candidate) and not re.search(pattern, candidate)) or re.fullmatch(r'[A-Za-z]{1,2}', candidate) :
                row["parish"] = candidate
        else:
            if re.search(r'[A-Z]', candidate) and re.search(r'[a-z]', candidate) and not re.search(pattern, candidate) and len(candidate) <= 25:
                row["parish"] = candidate    
    return row
surname_list = surname_list.apply(lambda row: firms_parishes_(row, initials = True, comma=True), axis=1)
surname_list = surname_list.apply(lambda row: firms_parishes_(row, initials = True,comma=False), axis=1)
surname_list = surname_list.apply(lambda row: firms_parishes_(row, initials = False,comma=True), axis=1)

parish_num = pd.DataFrame(surname_list["parish"].unique())
parish_firm = parish_num[parish_num[0].str.contains(pattern, regex = True)] 
parish_num = parish_num[~parish_num[0].isin(parish_firm[0])] #Actual parishes 
surname_list = surname_list.apply(remove_firms_from_parish, axis=1) #Control for eventuel mistakes after updating the dataframe

#Parish must be different from occupation and eventual first names or second last names
surname_list["parish"] = surname_list.apply(lambda x: "" if x["parish"].lower() == x["occ_reg"].lower() else x["parish"], axis = 1)
surname_list["parish"] = surname_list.apply(lambda x: "" if x["parish"] == x["initials"] and not re.search(r'\.', x["initials"]) and any(name == x["initials"] for name in first_names.values) else x["parish"], axis = 1 )
surname_list["parish"] = surname_list.apply(lambda x: "" if x["parish"] == x["second_last_name"] and x["parish"] not in parish_dict_known else x["parish"], axis = 1)
surname_list["second_last_name"] = surname_list.apply(lambda x: "" if x["parish"] == x["second_last_name"] and x["parish"] in parish_dict_known else x["second_last_name"], axis = 1)


#Double check if we have initials or parishes for firms
mask = (
    (surname_list["initials"] == surname_list["parish"]) &
    (surname_list["parish"] != "") &
    (surname_list["firm_dummy"] == 1) &
    (surname_list.apply(lambda x: x["line_complete"].count(str(x["initials"])), axis=1) == 1)
)
surname_list.loc[mask, "initials"] = ""

final_set = surname_list[["page","column","row","line","line_complete","index","split","firm_dummy","estate_dummy","last_name","best_match","initials","occ_reg","occ_reg_2","unique_key",'income','income_1','income_2']]
final_set.to_csv("final_set_dpsk_whole.csv")

surname_list.to_csv("aaa_5.csv")

surname_list = pd.read_csv("aaa_5.csv")
surname_list = surname_list.fillna("")
#####################################################################################
double_count_in = surname_list[(surname_list["parish"] == surname_list["initials"]) & 
                    (surname_list.apply(lambda x: str(x["line_complete"]).count(str(x["initials"])), axis=1) == 1) &
                    (surname_list["initials"] != "") & (surname_list["firm_dummy"] == 0)]



def first_clean(row):
    occ_ = row["occ_reg"]
    initials = row["initials"]
    line = row["line_complete"]
    last_name = row["last_name"]
    if occ_ != "":
        pos_occ_ = line.lower().index(occ_)
        pos_init = line.index(initials)
        if pos_occ_ and pos_init:
            if pos_occ_ > pos_init:
                row["parish"] = ""
                return row
            if pos_occ_ < pos_init:
                row["initials"] = ""
                line_split_space = line.split()
                line_split_space = [ch.replace(",","").strip() for ch in line_split_space]
                candidate = [word for word in line_split_space if word in first_names.values and word != last_name and len(word) > 2]
                if candidate:
                    row["initials"] = candidate[0]
                return row
    return row 

double_count_in = double_count_in.apply(first_clean, axis=1)
surname_list.update(double_count_in)
double_count_in = double_count_in[(double_count_in["parish"] == double_count_in["initials"]) & 
                    (double_count_in.apply(lambda x: str(x["line_complete"]).count(str(x["initials"])), axis=1) == 1) &
                    (double_count_in["initials"] != "") & (double_count_in["firm_dummy"] == 0)]


def double_count_init_par(row):
    initials = row["initials"]
    line = row["line_complete"]  # sicurezza
    last_name = row["last_name"]

    #if row["parish"] == initials and line.count(str(initials)) == 1 and initials != "" and row["firm_dummy"] == 0:
        #line_split = [x.strip() for x in line.split(",")]
    line_split = [x.strip() for x in line.split(",")]        
    if last_name != "" and any(w.strip().islower() for w in line.split()) and re.search(r'[A-Z][a-z]', line):
        # trova la posizione dell'elemento che contiene le initials
        pos_candidates = [i for i, x in enumerate(line_split) if initials in x]
        if pos_candidates:
            pos_initial = pos_candidates[0]  # prendi il primo match

            # controlla se il pezzo precedente contiene parole lowercase
            if pos_initial > 0 and any(word.islower() for word in line_split[pos_initial - 1].split()):
                row["initials"] = ""

                if len(line_split) > 1:
                    if line_split[1].strip() in first_names.values:
                        row["initials"] = line_split[1].strip()
    elif last_name != "" and any(word.strip() in first_names.values for word in line_split):
        candidate = [w for w in line_split if w in first_names.values and w != last_name and len(w) > 2]
        candidate = ' '.join(candidate).strip()
        row["initials"] = candidate
    return row

double_count_in = double_count_in.apply(double_count_init_par, axis=1)
surname_list.update(double_count_in)
double_count_in = double_count_in[(double_count_in["parish"] == double_count_in["initials"]) & 
                    (double_count_in.apply(lambda x: str(x["line_complete"]).count(str(x["initials"])), axis=1) == 1) &
                    (double_count_in["initials"] != "") & (double_count_in["firm_dummy"] == 0)]


def cut_part(row):
    init = row["initials"]
    line = row["line_complete"]
    last_name = row["last_name"]
    pos_init = line.index(init)
    pos = pos_init + len(init)
    line_cut = line[pos:]
    pos_init_split = [x for x in line.split(",") if init in x]
    pos_init_split = pos_init_split[0]
    pos_init_split = line.split(",").index(pos_init_split)
    if any(word.strip().islower() for word in line_cut.split()) or any(word.strip().islower() for word in line_cut.split(",")):
        row["parish"] = ""
    else:
        row["initials"] = ""
        line_split_space = line.split()
        line_split_space = [ch.replace(",","").strip() for ch in line_split_space]
        candidate = [word for word in line_split_space if word in first_names.values and word != last_name and len(word) > 2]
        if candidate:
            row["initials"] = candidate[0]    
    return row

double_count_in = double_count_in.apply(cut_part, axis=1)
surname_list.update(double_count_in)
double_count_in = double_count_in[(double_count_in["parish"] == double_count_in["initials"]) & 
                    (double_count_in.apply(lambda x: str(x["line_complete"]).count(str(x["initials"])), axis=1) == 1) &
                    (double_count_in["initials"] != "") & (double_count_in["firm_dummy"] == 0)]


double_count_in["parish"] = double_count_in.apply(lambda x: "" if len(x["line_complete"].split(",")) == 3 and x["last_name"] != "" else x["parish"], axis = 1)
surname_list.update(double_count_in)



#mnage the case where the parish should only appear in the initials and there is no actual parish
def avoid_double_count_init(row):
    init_ = row["initials"]
    parish = row["parish"]
    line = row["line_complete"]
    occ_ = row["occ_reg"]
    if (re.search(r'(?:[A-Z]\.|[A-Z][a-z]{1,2}\.)', parish) and parish != "" and parish.rstrip(",") in init_ and line.count(parish) == 1 and occ_ == "" and row["split"] == 3 and row["firm_dummy"] == 0
        and not re.search(r'\bfru|anke|froken', line) and row["estate_dummy"] == 0):
        pos_init = line.index(init_)
        line_cut = line[pos_init: pos_init + len(init_)-1]
        row["parish"] = "" if not re.search(r',',line_cut) else row["parish"]          
        
    return row
surname_list = surname_list.apply(avoid_double_count_init, axis = 1)


#Manage the case where there are only initials and they are wrongly mistaken as parishes
def manage_wrong_parish(row):
    init_ = row["initials"]
    parish = row["parish"]
    line = row["line_complete"]
    occ_ = row["occ_reg"]
    ln = row["last_name"]
    if occ_ == "" and row["split"] in [1,3] and row["firm_dummy"] == 0 and row["estate_dummy"] == 0 and parish != "" and init_ == "" and re.search(r'[A-Z]\.', line):
        pos_par = line.index(parish)
        line_cut = line[:pos_par]
        word = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ-]+", line_cut)[-1].rstrip(",").strip()
        if ln in word and ln != "":
            row["initials"] = parish
            row["parish"] = ""
    return row

surname_list = surname_list.apply(manage_wrong_parish, axis = 1)


surname_list.to_csv("aaa_6_final.csv")
surname_list = pd.read_csv("aaa_6_final.csv")
surname_list = surname_list.fillna("")

#########################################################################################
#Parish quality check
#########################################################################################
# 1. Import the extended clean list of dataframe fitted for 1912 with 1912 parishes matched to the clean list and 1912 locations added 
parish_only_matched = pd.read_csv("final_parish_csv_quality_check.csv")

#Update the list of known abbreviations
stockholm_known_par = surname_list[(surname_list["municipality"] == "Stockholm") & (surname_list["parish"] != "") & (surname_list["split"].isin([1,3])) & (surname_list["firm_dummy"] == 0) & (surname_list["estate_dummy"] == 0) &
                                   ((surname_list["index"] != "A1"))]

stockholm_known_par = stockholm_known_par[["parish","municipality"]].drop_duplicates()
stockholm_known_par["parish_old"] = stockholm_known_par["parish"]
stockholm_known_par["parish"] = stockholm_known_par["parish"].apply(lambda x: re.sub(r'\b\.\s', "", x))
stockholm_known_par["parish"] = stockholm_known_par["parish"].apply(lambda x: x.strip())
stockholm_known_par["parish"] = stockholm_known_par["parish"].apply(lambda x: re.sub(r'\(|\)', "", x))
stockholm_known_par["parish"] = (
    stockholm_known_par["parish"]
    .fillna("")  # evita NaN
    .str.replace(r"^[^A-Za-zÀ-ÖØ-öø-ÿ]+", "", regex=True)
)
def cleaned_parish(row, dict_=parish_dict_known):
    parish = str(row["parish"])
    parish_old = str(row["parish_old"])
    if "Stockholm" in row["municipality"] and parish_old not in parish_only_matched["parish_old"].values:
        parish_letters = "".join(re.findall(r"[A-Za-z]", parish))
        for key, value in dict_.items():
            key_letters = "".join(re.findall(r"[A-Za-z]", key))
            if abs(len(key) - len(parish)) <= 1 and parish_letters == key_letters:
                return value  # 👈 solo stringa
            
    return ""
stockholm_known_par["matched_parish"] = ""
stockholm_known_par["matched_parish"] = stockholm_known_par.apply(cleaned_parish, axis=1)
stockholm_known_par = stockholm_known_par[stockholm_known_par["matched_parish"] != ""]
parish_only_matched = pd.concat([parish_only_matched, stockholm_known_par], axis = 0)



# 2. Get the second parish list. The one which is updated year by year with information
df_parish_added_year_by_year = pd.read_csv("df_extra_parish_iterative_check.csv")

compare_group = parish_only_matched[["parish_old","matched_parish","municipality"]]
compare_group = compare_group.rename(columns = {"parish_old":"parish"})
compare_group = compare_group.drop_duplicates()



# 3. Create the third parish list. This one is created taking the extracted parishes in the book, remove those which are in the previous list, and match the residuals on the clean list
parish_mapped = pd.read_csv("parish_county.csv")

parish_mapped = parish_mapped.rename(columns={"parish": "Parish", "county": "municipality"})
parish_mapped["Parish"] = (
    parish_mapped["Parish"] 
    .str.replace("ö", "o")
    .str.replace("ä", "a")
    .str.replace("à", "a")
    .str.replace("å", "a")
    .str.replace("Ö", "O")
    .str.replace("Ä", "A")
    .str.replace("Å","A")
)
parish_mapped["municipality"] = (
    parish_mapped["municipality"] 
    .str.replace("ö", "o")
    .str.replace("ä", "a")
    .str.replace("à", "a")
    .str.replace("å", "a")
    .str.replace("Ö", "O")
    .str.replace("Ä", "A")
    .str.replace("Å","A")
)
parish_mapped["Parish"] = parish_mapped["Parish"].str[0].str.upper() + parish_mapped["Parish"].str[1:]

parish_analyzed = surname_list[["municipality","parish"]].drop_duplicates()
parish_analyzed["parish_old"] = parish_analyzed["parish"]
parish_analyzed = parish_analyzed[parish_analyzed["parish"].apply(lambda x: not re.search(r'\d',x))]
parish_analyzed["parish"] = parish_analyzed["parish"].apply(lambda x: re.sub(r'\b\.\s', "", x))
parish_analyzed["parish"] = parish_analyzed["parish"].apply(lambda x: x.strip())
parish_analyzed["parish"] = parish_analyzed["parish"].apply(lambda x: re.sub(r'\(|\)', "", x))
parish_analyzed["parish"] = (
    parish_analyzed["parish"]
    .fillna("")  # evita NaN
    .str.replace(r"^[^A-Za-zÀ-ÖØ-öø-ÿ]+", "", regex=True)
)
parish_analyzed = parish_analyzed[
    (~parish_analyzed["parish"].isin(parish_only_matched["parish_old"])) &
    (~parish_analyzed["parish"].isin(df_parish_added_year_by_year["parish_old"])) &
    (parish_analyzed["parish"] != "") &
    (
        parish_analyzed["parish"].apply(
            lambda s: (
                (letters := ''.join(re.findall(r'[A-Za-z]', str(s))))
                and not any(letters == ''.join(re.findall(r'[A-Za-z]', str(key))) for key in parish_dict_known.keys())
                and letters not in parish_only_matched["parish_old"].values
                and not any(letters == ''.join(re.findall(r'[A-Za-z]', str(word))) for word in df_parish_added_year_by_year["parish"].values) #before there was parish_old
            )
        )
    )
]





#This section performs a test on the parishes which are not in the list 
def fuzzy_match_rapidfuzz(x, choices):
    try:
        match = process.extractOne(x, choices, scorer=fuzz.token_sort_ratio)
        if match is None:
            return (None, 0, None)
        # match è tipo ('stringa_trovata', score, index)
        return match  
    except Exception:
        return (None, 0, None)

def check_on_parishes(row):
    parish_ = str(row["parish"])
    municip_, score, index = fuzzy_match_rapidfuzz(row["municipality"], parish_mapped["municipality"])
    municip_ = municip_ if score >= 85.5 else row["municipality"]

    # subset del municipality
    subgroup = parish_mapped[parish_mapped["municipality"] == municip_]

    # 1° tentativo: match dentro al municipio
    if not subgroup.empty:
        match = fuzzy_match_rapidfuzz(parish_, subgroup["Parish"])
        if match and len(match) == 3:  # evita unpack error
            best_match, score, index = match
            if score >= 85.5 and best_match is not None:
                row["matched_parish"] = best_match
                return row

    # 2° tentativo: match sull’intero dataset
    match = fuzzy_match_rapidfuzz(parish_, parish_mapped["Parish"])
    if match and len(match) == 3:
        best_match, score, index = match
        if score > 85.5 and best_match is not None:
            row["matched_parish"] = best_match
            return row
        
    # 3° tentativo: match sull’intero dataset con una parish ripulita
    parish_2 = re.sub(r'(\w+)\s*-\s*(\w+)', r'\1\2', parish_)
    match = fuzzy_match_rapidfuzz(parish_2, parish_mapped["Parish"])
    if match and len(match) == 3:
        best_match, score, index = match
        if score > 85.5 and best_match is not None:
            row["matched_parish"] = best_match
            return row        
    # fallback: nessun match
    row["matched_parish"] = ""
    return row


parish_analyzed = parish_analyzed.apply(check_on_parishes, axis = 1)
parish_analyzed = parish_analyzed[parish_analyzed["matched_parish"] != ""]

surname_list["parish"] = surname_list["parish"].apply(lambda s: s if s in parish_only_matched["parish_old"].values or #FIRST CLEAN PARISH LIST FITTED FOR 1912 
                                                      s in df_parish_added_year_by_year["parish_old"].values or #PARISH LIST IMPROVED YEAR BY YEAR 
                                                       s in parish_analyzed["parish_old"].values or #PARISH LIST CREATED BY MATCHING THE PARISH LIST WHICH ARE NOT IN THE CLEAN LIST WITH THE CLEAN LIST                                                       
                                                      any(''.join(re.findall(r'[A-Za-z]',s)) == ''.join(re.findall(r'[A-Za-z]',word)) and abs(len(s) - len(word)) <= 2 for word in df_parish_added_year_by_year["parish"].values)                                                       
                                                     or  any(''.join(re.findall(r'[A-Za-z]',s)) in word and abs(len(s) - len(word)) <= 2 for word in parish_only_matched["parish"].values)                                                       
                                                      else "")  #Quality check over the last name list
surname_list = surname_list.drop(columns = ["parish_cleaned_"])
surname_list = pd.merge(surname_list, compare_group, on = ["parish","municipality"], how = 'left')
surname_list["matched_parish"] = surname_list.apply(lambda s: s["parish"] if s["parish"] != "" and s["matched_parish"] == "" else "", axis = 1)


##################################################################################################
#Individuals
df_IT_2_complete = {"df_IT_2_complete_LN":certain["df_IT_2_complete"][certain["df_IT_2_complete"]["index"] != "A5"],
                    "df_IT_2_complete_A5":certain["df_IT_2_complete"][certain["df_IT_2_complete"]["index"] == "A5"]}

#Take first half and second half for every category
def df_FH_SH_FUNCT(df):
    idx_list = df.index.to_list()
    df["idx_true"] = df["index"]
    for pos, idx in enumerate(idx_list):
        index_act = df.at[idx,"index"]
        split_act = df.at[idx,"split"]
        if pos + 1 < len(df):
            nxt = idx_list[pos + 1]
            split_nxt = df.at[nxt, "split"]
        if split_act == 2:
            continue
        if split_act == 1 and split_nxt == 2:
            df.at[nxt,"idx_true"] = df.at[idx,"idx_true"]
        continue
    df_no_A5 = df[df["idx_true"] != "A5"]
    df_A5 = df[df["idx_true"] == "A5"]
    df = df.drop(columns = {"idx_true"})
    df_A5 = df_A5.drop(columns = {"idx_true"})
    df_no_A5 = df_no_A5.drop(columns = {"idx_true"})
    return df, df_no_A5, df_A5

df_IT_2_FH_SH = {}

certain["df_IT_2_FH_SH"], df_IT_2_FH_SH["df_IT_2_FH_SH_LN"], df_IT_2_FH_SH["df_IT_2_FH_SH_A5"] = df_FH_SH_FUNCT(certain["df_IT_2_FH_SH"])

df_IT_2_noise = {"df_IT_2_noise_LN":certain["df_IT_2_noise"][certain["df_IT_2_noise"]["index"] != "A5"],
                    "df_IT_2_noise_A5":certain["df_IT_2_noise"][certain["df_IT_2_noise"]["index"] == "A5"]}


#Firms
df_CF_complete = {"df_CF_complete_LN":certain["df_CF_complete"][certain["df_CF_complete"]["index"] != "A5"],
                    "df_CF_complete_A5":certain["df_CF_complete"][certain["df_CF_complete"]["index"] == "A5"]}

df_CF_FH_SH = {}
certain["df_CF_FH_SH"], df_CF_FH_SH["df_CF_FH_SH_LN"], df_CF_FH_SH["df_CF_FH_SH_A5"] = df_FH_SH_FUNCT(certain["df_CF_FH_SH"])


df_CF_noise = {"df_CF_noise_LN":certain["df_CF_noise"][certain["df_CF_noise"]["index"] != "A5"],
                    "df_CF_noise_A5":certain["df_CF_noise"][certain["df_CF_noise"]["index"] == "A5"]}


#Estate
df_CE_complete = {"certain_estate_complete_LN":certain["certain_estate_complete"][certain["certain_estate_complete"]["index"] != "A5"],
                    "certain_estate_complete_A5":certain["certain_estate_complete"][certain["certain_estate_complete"]["index"] == "A5"]}

df_CE_noise = {"certain_estate_noise_LN":certain["certain_estate_noise"][certain["certain_estate_noise"]["index"] != "A5"],
                    "certain_estate_noise_A5":certain["certain_estate_noise"][certain["certain_estate_noise"]["index"] == "A5"]}

df_CE_FH_SH = {}
certain["certain_estate_FH_SH"], df_CE_FH_SH["certain_estate_FH_SH_LN"], df_CE_FH_SH["certain_estate_FH_SH_A5"] = df_FH_SH_FUNCT(certain["certain_estate_FH_SH"])


#Potential second lines
P2L_spellout = {}

for i in ["A", "B", "C", "D"]:
    P2L_spellout[f"P2L_{i}_spellout"] = {
        f"potential_sec_line_{i}_complete": certain[f"potential_sec_line_{i}"][certain[f"potential_sec_line_{i}"]["split"] == 3],
        f"potential_sec_line_{i}_noise": certain[f"potential_sec_line_{i}"][certain[f"potential_sec_line_{i}"]["split"] == 0],
        f"potential_sec_line_{i}_FH": certain[f"potential_sec_line_{i}"][certain[f"potential_sec_line_{i}"]["split"].isin([1, 2])]
    }



#Certain noises
CN_spellout = {}
for i in ["A", "B"]:
    CN_spellout[f"CN_{i}_spellout"] = {
        f"CN_{i}_complete": certain[f"df_certain_noise_{i}"][certain[f"df_certain_noise_{i}"]["split"] == 3],
        f"CN_{i}_noise": certain[f"df_certain_noise_{i}"][certain[f"df_certain_noise_{i}"]["split"] == 0],
        f"CN_{i}_FH": certain[f"df_certain_noise_{i}"][certain[f"df_certain_noise_{i}"]["split"].isin([1, 2])]
    }


#Potential first lines
pot_first_line_spell_out = {"pot_first_line_complete":certain["pot_first_line"][certain["pot_first_line"]["split"] == 3],
                             "pot_first_line_FH_SH":certain["pot_first_line"][certain["pot_first_line"]["split"].isin([1,2])],
                             "pot_first_line_noise":certain["pot_first_line"][certain["pot_first_line"]["split"] == 0]}


#Get and spellout the duplicated lines
duplicated = remaining_lines[remaining_lines.duplicated(subset=["line"], keep=False)]
certain["duplicated"] = duplicated
remaining_lines = remaining_lines[~remaining_lines["unique_key"].isin(duplicated["unique_key"])]

duplicated_spellout = {"duplicated_complete":certain["duplicated"][certain["duplicated"]["split"] == 3],
                             "duplicated_FH_SH":certain["duplicated"][certain["duplicated"]["split"].isin([1,2])],
                             "duplicated_noise":certain["duplicated"][certain["duplicated"]["split"] == 0]}


#Spell uou the remaining lines
remaining_lines_spell_out = {"remaining_lines_complete":remaining_lines[remaining_lines["split"] == 3],
                             "remaining_lines_FH_SH":remaining_lines[remaining_lines["split"].isin([1,2])],
                             "remaining_lines_noise":remaining_lines[remaining_lines["split"] == 0]}
v = pd.DataFrame()
for key in certain:
    v = pd.concat([v,certain[key]], axis = 0)
duplicated = v[v.duplicated(subset=["unique_key"], keep=False)]





info = surname_list[(~surname_list["unique_key"].isin(v["unique_key"])) & (~surname_list["unique_key"].isin(remaining_lines["unique_key"]))]
info_2 = surname_list[(surname_list["unique_key"].isin(v["unique_key"])) & (surname_list["unique_key"].isin(remaining_lines["unique_key"]))]

duplicated_remaining_lines = remaining_lines[remaining_lines.duplicated(subset=["unique_key"], keep=False)]


duplicated_remaining_lines = remaining_lines[remaining_lines.duplicated(subset=["line"], keep=False)]


#surname_list = pd.merge(surname_list, list_parish_matched, on = "parish", how = 'left')
#surname_list = surname_list.fillna("")

final_set = surname_list[["page","column","row","line","line_complete","index","split","firm_dummy","estate_dummy","last_name","best_match","initials","occ_reg","occ_reg_2","municipality","parish","matched_parish","unique_key",'income','income_1','income_2']]

final_set.to_csv("final_set_1912_dpsk_whole.csv")




info = final_set[(final_set["parish"] == "") & (final_set["municipality"] == "Stockholm") & 
                 (final_set["firm_dummy"] == 0) & (final_set["estate_dummy"] == 0) &
                 (final_set["index"] != "A1") & (final_set["split"].isin([1,3]))]

info = final_set[(final_set["last_name"] == "")  & 
                 (final_set["firm_dummy"] == 0) & (final_set["estate_dummy"] == 0) &
                 (final_set["index"] != "A1") & (final_set["split"].isin([1,3]))]


