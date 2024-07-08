## Wikitionary jsonl dump to sqlite:

(https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl)

Narrow subset -> 2 main tables + 2 helper tables 
(words, part-of-speech, lang) + (word_forms for word, tag)

lines in source jsonl  : 1306029

parse time: 2.8119661000000002 secs.
db insert time: 8.8602965 secs.

main time: 10.4464539 secs.
(2 seperate threads for parsing and db commits)
5000 -> bulk size for transactions

db inserts (words + word_forms):
1276286 + 965908