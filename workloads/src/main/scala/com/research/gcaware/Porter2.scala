package com.research.gcaware

import java.util.regex.Pattern

/**
 * Porter2 (Snowball English) stemmer — a faithful port of pyporter2
 * (udfs/scalar/stem.py), used by the UDFBench `stem` UDF (q18).
 * See http://snowball.tartarus.org/algorithms/english/stemmer.html
 *
 * ponytail: kept a line-for-line translation of the Python rather than pulling a
 * stemming library — the benchmark must reproduce the Python's exact output, and
 * a third-party stemmer would drift on edge cases.
 */
object Porter2 {
  private val rExp = Pattern.compile("[^aeiouy]*[aeiouy]+[^aeiouy](\\w*)")
  private val ccyExp = Pattern.compile("([aeiouy])y")
  private val s1aExp = Pattern.compile("[aeiouy].")
  private val s1bExp = Pattern.compile("[aeiouy]")

  private val vowels = "aeiouy"

  // get_r1 with the exceptional prefixes; returns start of rExp group 1 (or len).
  private def getR1(word: String): Int = {
    if (word.startsWith("gener") || word.startsWith("arsen")) return 5
    if (word.startsWith("commun")) return 6
    val m = rExp.matcher(word)
    if (m.lookingAt()) m.start(1) else word.length
  }

  private def getR2(word: String): Int = {
    val r1 = getR1(word)
    val m = rExp.matcher(word)
    m.region(r1, word.length)
    if (m.lookingAt()) m.start(1) else word.length
  }

  private def endsWithShortSyllable(word: String): Boolean = {
    if (word.length == 2 && word.matches("[aeiouy][^aeiouy]")) return true
    word.matches(".*[^aeiouy][aeiouy][^aeiouywxY]")
  }

  private def isShortWord(word: String): Boolean =
    endsWithShortSyllable(word) && getR1(word) == word.length

  private def removeInitialApostrophe(word: String): String =
    if (word.startsWith("'")) word.substring(1) else word

  private def capitalizeConsonantYs(word: String): String = {
    val w = if (word.startsWith("y")) "Y" + word.substring(1) else word
    ccyExp.matcher(w).replaceAll("$1Y")
  }

  private def step0(word: String): String = {
    if (word.endsWith("'s'")) word.dropRight(3)
    else if (word.endsWith("'s")) word.dropRight(2)
    else if (word.endsWith("'")) word.dropRight(1)
    else word
  }

  private def step1a(word: String): String = {
    if (word.endsWith("sses")) return word.dropRight(4) + "ss"
    if (word.endsWith("ied") || word.endsWith("ies"))
      return if (word.length > 4) word.dropRight(3) + "i" else word.dropRight(3) + "ie"
    if (word.endsWith("us") || word.endsWith("ss")) return word
    if (word.endsWith("s")) {
      val preceding = word.dropRight(1)
      return if (s1aExp.matcher(preceding).find()) preceding else word
    }
    word
  }

  private val doubles = Seq("bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt")
  private def endsWithDouble(word: String): Boolean = doubles.exists(word.endsWith)

  private def step1bHelper(word: String): String = {
    if (word.endsWith("at") || word.endsWith("bl") || word.endsWith("iz")) return word + "e"
    if (endsWithDouble(word)) return word.dropRight(1)
    if (isShortWord(word)) return word + "e"
    word
  }
  private val s1bSuffixes = Seq("ed", "edly", "ing", "ingly")

  private def step1b(word: String, r1: Int): String = {
    if (word.endsWith("eedly")) return if (word.length - 5 >= r1) word.dropRight(3) else word
    if (word.endsWith("eed")) return if (word.length - 3 >= r1) word.dropRight(1) else word
    for (suffix <- s1bSuffixes) {
      if (word.endsWith(suffix)) {
        val preceding = word.dropRight(suffix.length)
        return if (s1bExp.matcher(preceding).find()) step1bHelper(preceding) else word
      }
    }
    word
  }

  private def step1c(word: String): String = {
    if (word.endsWith("y") || word.endsWith("Y")) {
      if (!vowels.contains(word.charAt(word.length - 2)) && word.length > 2)
        return word.dropRight(1) + "i"
    }
    word
  }

  // (end, repl, prev). prev empty = unconditional; else the stem must end with one of prev.
  private def step2Helper(word: String, r1: Int, end: String, repl: String, prev: Seq[String]): Option[String] = {
    if (word.endsWith(end)) {
      if (word.length - end.length >= r1) {
        val stem = word.dropRight(end.length)
        if (prev.isEmpty) return Some(stem + repl)
        for (p <- prev) if (stem.endsWith(p)) return Some(stem + repl)
      }
      Some(word)
    } else None
  }
  private val s2Triples: Seq[(String, String, Seq[String])] = Seq(
    ("ization", "ize", Nil), ("ational", "ate", Nil), ("fulness", "ful", Nil),
    ("ousness", "ous", Nil), ("iveness", "ive", Nil), ("tional", "tion", Nil),
    ("biliti", "ble", Nil), ("lessli", "less", Nil), ("entli", "ent", Nil),
    ("ation", "ate", Nil), ("alism", "al", Nil), ("aliti", "al", Nil),
    ("ousli", "ous", Nil), ("iviti", "ive", Nil), ("fulli", "ful", Nil),
    ("enci", "ence", Nil), ("anci", "ance", Nil), ("abli", "able", Nil),
    ("izer", "ize", Nil), ("ator", "ate", Nil), ("alli", "al", Nil),
    ("bli", "ble", Nil), ("ogi", "og", Seq("l")),
    ("li", "", Seq("c", "d", "e", "g", "h", "k", "m", "n", "r", "t")))

  private def step2(word: String, r1: Int): String = {
    for (t <- s2Triples) step2Helper(word, r1, t._1, t._2, t._3) match {
      case Some(a) if a.nonEmpty => return a
      case _ =>
    }
    word
  }

  private def step3Helper(word: String, r1: Int, r2: Int, end: String, repl: String, r2Necessary: Boolean): Option[String] = {
    if (word.endsWith(end)) {
      if (word.length - end.length >= r1) {
        if (!r2Necessary) return Some(word.dropRight(end.length) + repl)
        else if (word.length - end.length >= r2) return Some(word.dropRight(end.length) + repl)
      }
      Some(word)
    } else None
  }
  private val s3Triples: Seq[(String, String, Boolean)] = Seq(
    ("ational", "ate", false), ("tional", "tion", false), ("alize", "al", false),
    ("icate", "ic", false), ("iciti", "ic", false), ("ative", "", true),
    ("ical", "ic", false), ("ness", "", false), ("ful", "", false))

  private def step3(word: String, r1: Int, r2: Int): String = {
    for (t <- s3Triples) step3Helper(word, r1, r2, t._1, t._2, t._3) match {
      case Some(a) if a.nonEmpty => return a
      case _ =>
    }
    word
  }

  private val s4DeleteList = Seq("al", "ance", "ence", "er", "ic", "able", "ible", "ant",
    "ement", "ment", "ent", "ism", "ate", "iti", "ous", "ive", "ize")

  private def step4(word: String, r2: Int): String = {
    for (end <- s4DeleteList) {
      if (word.endsWith(end))
        return if (word.length - end.length >= r2) word.dropRight(end.length) else word
    }
    if (word.endsWith("sion") || word.endsWith("tion"))
      return if (word.length - 3 >= r2) word.dropRight(3) else word
    word
  }

  private def step5(word: String, r1: Int, r2: Int): String = {
    if (word.endsWith("l")) {
      if (word.length - 1 >= r2 && word.charAt(word.length - 2) == 'l') return word.dropRight(1)
      return word
    }
    if (word.endsWith("e")) {
      if (word.length - 1 >= r2) return word.dropRight(1)
      if (word.length - 1 >= r1 && !endsWithShortSyllable(word.dropRight(1))) return word.dropRight(1)
    }
    word
  }

  private val exceptionalForms = Map(
    "skis" -> "ski", "skies" -> "sky", "dying" -> "die", "lying" -> "lie",
    "tying" -> "tie", "idly" -> "idl", "gently" -> "gentl", "ugly" -> "ugli",
    "early" -> "earli", "only" -> "onli", "singly" -> "singl", "sky" -> "sky",
    "news" -> "news", "howe" -> "howe", "atlas" -> "atlas", "cosmos" -> "cosmos",
    "bias" -> "bias", "andes" -> "andes")

  private val exceptionalEarlyExitPost1a = Set(
    "inning", "outing", "canning", "herring", "earring", "proceed", "exceed", "succeed")

  def stem(word0: String): String = {
    if (word0.length <= 2) return word0
    var word = removeInitialApostrophe(word0)
    exceptionalForms.get(word).foreach(return _)
    word = capitalizeConsonantYs(word)
    val r1 = getR1(word)
    val r2 = getR2(word)
    word = step0(word)
    word = step1a(word)
    if (exceptionalEarlyExitPost1a.contains(word)) return word
    word = step1b(word, r1)
    word = step1c(word)
    word = step2(word, r1)
    word = step3(word, r1, r2)
    word = step4(word, r2)
    word = step5(word, r1, r2)
    word.replace("Y", "y")
  }
}
