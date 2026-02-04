# ocr_modules - Refactored OCR data processing pipeline
#
# Modules:
#   config            – Constants, regex patterns, dictionaries
#   utils             – Shared utility functions
#   data_loader       – Data loading and initial dataframe setup
#   last_name_matching– Surname matching algorithms (perfect, fuzzy, alt)
#   line_processing   – Line cleaning, splitting, residual extraction
#   initials_names    – Initials and first-name extraction
#   occupation        – Occupation extraction and fuzzy matching
#   income            – Income extraction and splitting
#   parish            – Parish extraction, mapping, quality check
#   location          – Location and municipality assignment
#   classification    – Certain-lines classification and potential-lines
#   firm_estate       – Firm and estate token handling
