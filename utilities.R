repl_invalid_chars <- function(x) {
  x |>
    # Replace any characters that are not numbers or letters with "_"
    stringr::str_replace_all("[^[:alnum:]]", "_") |>
    # If first character is a number, add a "_" to the beginning
    stringr::str_replace("([0-9]+)(.*)", "_\\1\\2")
}

filename_clean <- function(x, folder = FALSE) {
  x <- gsub("\\", ifelse(folder, "/", ""), x, fixed = TRUE)
  if (!folder) {
    x <- gsub("/", "", x, fixed = TRUE)
  }
  x <- gsub(":", "", x, fixed = TRUE)
  x <- gsub("*", "", x, fixed = TRUE)
  x <- gsub("?", "", x, fixed = TRUE)
  x <- gsub("\"", "", x, fixed = TRUE)
  x <- gsub("<", "LT", x, fixed = TRUE)
  x <- gsub(">", "GT", x, fixed = TRUE)
  gsub("|", "", x, fixed = TRUE)
}

cardinal_map <- function(rel) {
  left <- ifelse(rel$FromCardinality == 1, "||", "}|")
  right <- ifelse(rel$ToCardinality == 1, "||", "|{")
  paste0(left, "--", right)
}
