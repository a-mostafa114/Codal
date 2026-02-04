# -*- coding: utf-8 -*-
"""
config.py – Constants, regex patterns, and reference dictionaries.

All hard-coded patterns and look-up tables live here so that every other
module can simply ``from dpsk_modules.config import …``.
"""

# ── Firm regex pattern ──────────────────────────────────────────────────
FIRM_PATTERN = (
    r'Sparkassa|Pharmacia|Produktkompaniet|Norra Frivilliga Arbetshuset'
    r'|Mellersta & Norra Sveriges Angpannefor- ening|Siosteens|Social-Demokraten'
    r'|Maleriarbetarforbundet|Missionsforbunaet|Metallindustriarbetareforbundet'
    r'|Landtmannens Riksforbund|Traarbetareforbundet|Tvalkompaniet|Norra Station'
    r'|Pilgrimstads Andersmejeri|AB|Machinery|Exportaffar|Centralautomaten'
    r'|Pram- & Bogs|Sagverksforbundet|Bryggeriidkareforbundet|Credit|Sallskapet'
    r'|Elektriska|Handelsbanken|Pappersbruk|Sjomanshemmet|\bKredit|Sprithandelsbol'
    r'|\bAkt\.|Timmermansorden|Tomtrattskassa|Hypotekskassa|Societe General'
    r'|Schlesische Feuerversicherungs'
    r'|Rante- och Kapitalforsakringsanstalten|Olycksfallsforsakr\.'
    r'|Hotell|Centralbanken|Banque|Laval Separator|United Shoe'
    r'|Forlagsexpedition|Accumuslatoren|Affarssystem|Affarsbanken|Gesellschaft'
    r'|servicekassa|Spirituosabol|Assurance-Comp|Afdeln|Spritforsaljningsbol'
    r'|Mjolkcentral|Tegelindustri|C:o|Industriforbund|Express Comp'
    r'|Elektricitats-Ges|Coldinu Orden|Transmissionsverken|Pensionsfond'
    r'|National Versicherungs|Advokatsamfund|Publicistklubben|Generaldepot'
    r'|Lanekassa|Generaldepot|C:o Limited|Pupillkassan'
    r'|olycksfallsforsakringsanstalten|Lmtd|Kreditkassa|laskedrycksfabr\.'
    r'|generaldepot|pensionsfond|Olycksfallforsakringsanstalten|Stora Sallskapet'
    r'|Stadernas Allmanna|forsamlingen|hamnarbetskontor|hypotekskassa'
    r'|brandforsakringskontor|Schweizerische Unfallversicherungs-A.-G. Kh., 16850-16800'
    r'|Commercial Union|Elektricitetsv\.|Elektr\.-verk|A\s*\.B-\.|samfundet'
    r'|Petroleum|A\.\s*-B|organisationen|Stads|Centralforbundet|-verk,'
    r'|Hartzlimfabr|Cementgjuteri|fabriksbod|Borgerskapskassa|intressenter'
    r'|Korkfabrik|filial|Angbryggeri|Lysoljeaffar|Yllefabrik|verket'
    r'|hofding gre|Allm\.|Byra|Kungl\.|Foreningen|Armaturfabriken'
    r'|Forenade Industrier|besparingsskog|jarnvagsdrift|Brandforsakringsinrattn'
    r'|tradgardinfabriken|Andels|haradsallmanning|u\.p\.\a|Nya Ullspinneri'
    r'|Petroleumselskab|Goteborgssystemet|Hushallsskolan|Bolag|Stadspark'
    r'|Sparbanken|firma|sparkasse|stiftelse|villastad|tomtrattsk|arbetshuset'
    r'|foren|kaffebranneri|Insurance|Bolaget|Banken|u\. p\. a\.|stationer'
    r'|A\.B\.|Company|Ltd|Filial|Hogfjallspensionat'
    r'|jarnvag(?![A-Za-z])|Jarnvag(?![A-Za-z])|Koop\.|Kooperativa|Gasverket'
    r'|Mjolkforsaljn|Vattenledning|\b[A-Za-z]{2,}sverk[^A-Za-z]+'
    r'|\b[A-Za-z]{5,}fabrik\b|Bank(?![A-Za-z])|A-B|A- B|-B\.|A\.-B\.|c:o'
    r'|A-\.B|Svenska|svenska|forening|Sthlms|sthlms|-akt\.-bol\.|-akt\.'
    r"|akt\.-|-b\.|societ|aktie|Aktie|-bol|bol\.|bolag|Bostad"
    r"|bank(?![A-Za-z])|L:td|a\.-b\.|akt\.-bol\.|fonden"
)

# ── Estate regex pattern ────────────────────────────────────────────────
ESTATE_PATTERN = r'st\.-hus|starbh|sterbh|starkbhus|starb-|starb\'h|sta bh'

# ── Initials regex pattern ──────────────────────────────────────────────
INITIALS_PATTERN = (
    r'\b(?:[A-Z]{1,3}\.|[A-Z]:\w+|[A-Z]:\s|[A-Z]:,|[A-Z][a-z]\.'
    r'|[A-Z][a-z]{2}\.|\. [A-Z]\.)'
)

# ── Words that are not an occupation ────────────────────────────────────
NO_OCC_LIST = ["hustru", "fru", "fröken", "änkefru"]

# ── Parish abbreviation dictionary ──────────────────────────────────────
PARISH_DICT_KNOWN = {
    "N.":       "Nikolai",
    "Kt.":      "Katarina",
    "M.":       "Maria",
    "Kh.":      "Kungsholms",
    "Kl.":      "Klara",
    "J.":       "Jakobs o. Johannes",
    "A.":       "Adolf Fredriks",
    "H.":       "Hedvig Eleonora",
    "E.":       "Engelbrekts",
    "O.":       "Oscars",
    "G.":       "Gustaf Wasa",
    "Dj:holm":  "Djursholm",
    "Mt.":      "Matteus",
}

# ── Cities / parishes used for later cleaning ───────────────────────────
CITIES_PAR = [
    "Asarum", "Alfvesta", "Rimbo", "Herrljunga", "Kopenhamn", "Kungsor",
    "Karlskrona", "Saltsjobaden", "Saltsjobaden", "Sundbyberg", "Harene",
    "Stockholms", "Haga", "Skon", "Hoby", "Bracke", "Ahus", "Svardsjo",
    "Vinslof", "Hogsby", "Ekby", "Billeberga", "Mellosa", "Morlunda",
    "Stockholm", "Hvena", "Kyrkefalla", "Sandby", "Lidingo",
    "Liljeholmen", "Stentorp", "Rimbo", "Alno", "Saltsjobaden",
    "Sundbyberg", "Stockholms stad", "Botkyrka", "Goteborg", "Karlskrona",
    "Sthlm", "Kopenhamn", "Kyrkhult", "Asarum", "Hjortsberga", "Visby",
    "Hogran", "Voxna", "Loos", "Orgryte", "Smedsasen", "Malilla",
    "Finja", "Hassleholm", "Perstorp", "Farlof", "Glimakra", "Hjarsa",
    "Brosarp", "Hastveda", "Elmhult", "Alfvesta", "Asheda", "Bjuf",
    "Raus", "Skraflinge", "Korpilombolo", "Jukkasjarvi", "Morko",
    "Bettna", "Oxelosund", "Vrena", "By", "Kungsor", "Tranemo",
    "Herrljunga", "Nasby", "Almby", "Eggby", "Kyrkefalla",
]
