from sparkstart import scon, spark

def a(scon, spark, path):
    rdd = scon.textFile(path)
    return rdd

def b(rdd):
    print("\n=== Aufgabe 6b ===")
    total_chars = rdd.map(lambda line: len(line)).reduce(lambda a, b: a + b)
    print("Anzahl aller Zeichen im Text:", total_chars)
    return total_chars

def c(rdd, top_n=20):
    print("\n=== Aufgabe 6c ===")
    chars = rdd.flatMap(lambda line: list(line))
    freq = chars.map(lambda c: (c, 1)).reduceByKey(lambda a, b: a + b)
    top_chars = freq.takeOrdered(top_n, key = lambda x: -x[1])
    
    print(f"\n Top {top_n} häufigste Zeichen:")
    for ch, count in top_chars:
        print(f"'{ch}': {count}")
    
    unique_chars = chars.distinct().collect()
    print("\nAnzahl verschiedener Zeichen:", len(unique_chars))
    return freq

def d(rdd, top_n=20):
    print("\n=== Aufgabe 6d ===")
    from string import punctuation
    def remove_punct(line):
        return "".join([" " if ch in punctuation else ch for ch in line])
    
    cleaned = rdd.map(remove_punct)
    
    words = (cleaned
             .flatMap(lambda line: line.split())
             .map(lambda w: w.lower())
             .filter(lambda w: w != ""))
    
    word_freq = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
    top_words = word_freq.takeOrdered(top_n, key=lambda x: -x[1])
    
    print(f"\nTop {top_n} häufigste Wörter:")
    for w, c in top_words:
        print(f"{w}: {c}")
    
    return word_freq

def e(scon,spark, path, top_n=20):
    rdd = a(scon, spark, path)
    return d(rdd, top_n)

def main(scon, spark):
    rdd = a(scon, spark, "/data/texte/test/robinsonCrusoe.txt")
    b(rdd)
    c(rdd)
    d(rdd)
    e(scon, spark, "/data/texte/test/DonQuijote.txt")

if __name__ == "__main__":
    main(scon, spark)