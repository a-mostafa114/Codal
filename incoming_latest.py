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
from openai import OpenAI
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
dirty_last_names_list = pd.read_excel("C:/Users/vapa2530/Desktop/Surnames/Last_names_to_update_DR.xlsx", sheet_name="Sheet2")
dirty_last_names_list["last_name"] = dirty_last_names_list["line"].str.split(",", n= 1).str[0].str.strip()
dirty_last_names_list = dirty_last_names_list[["last_name","last_name_clean"]].drop_duplicates(subset = "last_name")
dirty_last_names_dict = dirty_last_names_list.set_index("last_name")["last_name_clean"].to_dict()



#Original death register
df_death_reg_orig = pd.read_stata("C:/Users/vapa2530/Desktop/lname_deathreg.dta")


#The updated death register that we are going to use
df_death_reg_unacc = pd.read_csv("C:/Users/vapa2530/Desktop/Surnames/Updated_DR.csv")
df_death_reg_unacc = df_death_reg_unacc.sort_values(by="last_name", key=lambda x: x.str.len(),ascending=False)
df_death_reg_unacc = df_death_reg_unacc[~df_death_reg_unacc["last_name"].str.contains("hustru")]

#Import first names
first_names, meta = pyreadstat.read_dta("C:/Users/vapa2530/Desktop/Surnames/First_name/Burial_names.dta")
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
df_1912 = pd.read_csv("C:/Users/vapa2530/Downloads/taxeringskalender_1912_02.csv")
main_dataframe = df_1912 

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
pattern = r'Sparkassa|Pharmacia|Produktkompaniet|Norra Frivilliga Arbetshuset|Mellersta & Norra Sveriges Angpannefor- ening|Siosteens|Social-Demokraten|Maleriarbetarforbundet|Missionsforbunaet|Metallindustriarbetareforbundet|Landtmannens Riksforbund|Traarbetareforbundet|Tvalkompaniet|Norra Station|Pilgrimstads Andersmejeri|AB|Machinery|Exportaffar|Centralautomaten|Pram- & Bogs|Sagverksforbundet|Bryggeriidkareforbundet|Credit|Sallskapet|Elektriska|Handelsbanken|Pappersbruk|Sjomanshemmet|\bKredit|Sprithandelsbol|\bAkt\.|Timmermansorden|Tomtrattskassa|Hypotekskassa|Societe General|Schlesische Feuerversicherungs|Rante- och Kapitalforsakringsanstalten|Olycksfallsforsakr\.|Hotell|Centralbanken|Banque|Laval Separator|United Shoe|Forlagsexpedition|Accumuslatoren|Affarssystem|Affarsbanken|Gesellschaft|servicekassa|Spirituosabol|Assurance-Comp|Afdeln|Spritforsaljningsbol|Mjolkcentral|Tegelindustri|C:o|Industriforbund|Express Comp|Elektricitats-Ges|Coldinu Orden|Transmissionsverken|Pensionsfond|National Versicherungs|Advokatsamfund|Publicistklubben|Generaldepot|Lanekassa|Generaldepot|C:o Limited|Pupillkassan|olycksfallsforsakringsanstalten|Lmtd|Kreditkassa|laskedrycksfabr\.|generaldepot|pensionsfond|Olycksfallforsakringsanstalten|Stora Sallskapet|Stadernas Allmanna|forsamlingen|hamnarbetskontor|hypotekskassa|brandforsakringskontor|Schweizerische Unfallversicherungs-A.-G. Kh., 16850-16800|Commercial Union|Elektricitetsv\.|Elektr\.-verk|A\s*\.B-\.|samfundet|Petroleum|A\.\s*-B|organisationen|Stads|Centralforbundet|-verk,|Hartzlimfabr|Cementgjuteri|fabriksbod|Borgerskapskassa|intressenter|Korkfabrik|filial|Angbryggeri|Lysoljeaffar|Yllefabrik|verket|hofding gre|Allm\.|Byra|Kungl\.|Foreningen|Armaturfabriken|Forenade Industrier|besparingsskog|jarnvagsdrift|Brandforsakringsinrattn|tradgardinfabriken|Andels|haradsallmanning|u\.p\.\a|Nya Ullspinneri|Petroleumselskab|Goteborgssystemet|Hushallsskolan|Bolag|Stadspark|Sparbanken|firma|sparkasse|stiftelse|villastad|tomtrattsk|arbetshuset|foren|kaffebranneri|Insurance|Bolaget|Banken|u\. p\. a\.|stationer|A\.B\.|Company|Ltd|Filial|Hogfjallspensionat|jarnvag(?![A-Za-z])|Jarnvag(?![A-Za-z])|Koop\.|Kooperativa|Gasverket|Mjolkforsaljn|Vattenledning|\b[A-Za-z]{2,}sverk[^A-Za-z]+|\b[A-Za-z]{5,}fabrik\b|Bank(?![A-Za-z])|A-B|A- B|-B\.|A\.-B\.|c:o|A-\.B|Svenska|svenska|forening|Sthlms|sthlms|-akt\.-bol\.|-akt\.|akt\.-|-b\.|societ|aktie|Aktie|-bol|bol\.|bolag|Bostad|bank(?![A-Za-z])|L:td|a\.-b\.|akt\.-bol\.|fonden'

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


# (file continues...)
