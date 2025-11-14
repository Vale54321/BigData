from sparkstart import scon, spark

def readAllLanguages(scon, lang_file):
    return (scon.textFile(lang_file)
              .map(lambda line: line.strip().split(","))
              .filter(lambda x: len(x) == 2)
              .map(lambda x: (x[1].lower(), x[0].lower())))

def readAllTexts(scon, directory, partitions=1000):
    import os
    
    rdd = scon.wholeTextFiles(directory, minPartitions=partitions)
    rdd = rdd.map(lambda x: (os.path.basename(x[0]), x[1]))
    return rdd


def clean_and_split(rdd):
    from string import punctuation
    
    def clean_text(text):
        for ch in punctuation + "\n\t\r":
            text = text.replace(ch, " ")
        return text

    return (rdd.flatMap(lambda x: [(x[0], w.lower())
                                   for w in clean_text(x[1]).split(" ")
                                   if w.strip() != ""]))


def unique_words_per_file(word_rdd):
    return (word_rdd.distinct()
                    .map(lambda x: (x[1], x[0])))


def join_with_languages(word_file_rdd, lang_rdd):
    return word_file_rdd.join(lang_rdd)



def count_words_per_language(joined_rdd):
    return (joined_rdd
            .map(lambda x: ((x[1][0], x[1][1]), 1))
            .reduceByKey(lambda a, b: a + b))



def detect_language_per_file(count_rdd):
    return (count_rdd
            .map(lambda x: (x[0][0], (x[0][1], x[1])))
            .reduceByKey(lambda a, b: a if a[1] > b[1] else b)
            .map(lambda x: (x[0], x[1][0])))


def count_texts_per_language(lang_detected_rdd):
    return (lang_detected_rdd
            .map(lambda x: (x[1], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortBy(lambda x: -x[1]))


def detect_languages(scon, text_dir, lang_file, partitions=1000):
    import time
    
    start_time = time.time()

    lang_rdd = readAllLanguages(scon, lang_file)

    texts_rdd = readAllTexts(scon, text_dir, partitions=partitions)

    words_rdd = clean_and_split(texts_rdd)
    unique_rdd = unique_words_per_file(words_rdd)
    joined_rdd = join_with_languages(unique_rdd, lang_rdd)
    counts_rdd = count_words_per_language(joined_rdd)
    detected_rdd = detect_language_per_file(counts_rdd)
    summary_rdd = count_texts_per_language(detected_rdd)

    detected = detected_rdd.collect()
    summary = summary_rdd.collect()

    end_time = time.time()
    duration = end_time - start_time

    print(f"Abgeschlossen in {duration:.2f} Sekunden\n")

    print("Erkannte Sprachen pro Datei:")
    for f, lang in detected[:20]:
        print(f"{f}: {lang}")

    print("\nGesamtanzahl Texte pro Sprache:")
    for lang, count in summary:
        print(f"{lang}: {count}")

def main(scon, spark):
    detect_languages(scon,
                     text_dir="/data/texte/txt",
                     lang_file="/data/texte/languages.txt",)

if __name__ == "__main__":
    main(scon, spark)