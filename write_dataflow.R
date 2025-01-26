write_dataflow <- function(Dataflow, DataflowId, Workspace, WorkspaceId, access_token) {
  message("  Df: ", Dataflow)
  dataflow_path <- file.path(Workspace, "Dataflows", Dataflow)
  if (!dir.exists(dataflow_path)) {
    dir.create(dataflow_path, recursive = TRUE)
  }

  dataflow_definitions_raw <- httr::GET(
    paste0("https://api.powerbi.com/v1.0/myorg/groups/", WorkspaceId, "/dataflows/", DataflowId),
    config = qiverse.powerbi:::get_auth_header(access_token),
    httr::content_type_json()) |>
    httr::content(as = "text") |>
    jsonlite::prettify(indent = 2)

  dataflow_definitions <- jsonlite::fromJSON(dataflow_definitions_raw,
                                             simplifyVector = FALSE)

  # Dataflow queries are stored in a single file, need to separate to separate queries
  dataflow_m_code <- dataflow_definitions$`pbi:mashup`$document
  query_per_table <- strsplit(dataflow_m_code, ";", fixed = TRUE)[[1]]
  query_per_table <- query_per_table[2:(length(query_per_table)-1)]
  # Ignore sections of the query that aren't defining tables or parameters
  query_per_table <- query_per_table[grepl("=", query_per_table, fixed = TRUE)]

  query_names <- stringr::str_extract(query_per_table, 'shared (#")?(.*)?(")? =', group = 2) |>
    stringr::str_remove("(#|\")") |>
    stringr::str_remove(" = (.*)") |>
    stringr::str_remove("/") |>
    filename_clean() |>
    paste0(".fs")

  curr_files <- list.files(path = dataflow_path)
  curr_files <- curr_files[!(curr_files %in% c("ERD.pdf", "README.md"))]
  removed <- curr_files[!(curr_files %in% query_names)]
  if (length(removed) > 0) {
    purrr::walk(removed, function(rm_file) { unlink(file.path(dataflow_path, rm_file),
                                                    recursive = TRUE) })
  }

  purrr::walk2(query_names, query_per_table, function(query_name, query) {
    cat(query, file = file.path(dataflow_path, query_name),
        sep = "\n", append = FALSE)
  })

  dataflow_tables <- purrr::map_chr(dataflow_definitions$entities, function(entity) {
    # Linked Dataflow tables do not have type data available, so skip
    if (entity$`$type` != "LocalEntity") {
      return("")
    }
    name_types <- purrr::map_chr(entity$attributes, function(attributes) {
      paste(attributes$dataType, repl_invalid_chars(attributes$name))
    })
    stringr::str_glue(
      '"{entity$name}" {{',
      paste(name_types, collapse = "\n"),
      "}}", .sep = "\n"
    )
  })
  # If the dataflow only contains linked tables, then there are no type definitions for the ERD
  dataflow_tables <- dataflow_tables[dataflow_tables != ""]
  if (length(dataflow_tables) > 0) {
    cat(paste(c("```mermaid", "erDiagram", dataflow_tables, "```"), collapse="\n"),
        file = file.path(dataflow_path, "README.md"), append = FALSE)
  } else {
    if (file.exists(file.path(dataflow_path, "README.md"))) {
      file.remove(file.path(dataflow_path, "README.md"))
    }
    if (file.exists(file.path(dataflow_path, "ERD.pdf"))) {
      file.remove(file.path(dataflow_path, "ERD.pdf"))
    }
  }

  cat(dataflow_definitions_raw,
      file = file.path(dataflow_path, "model.json"),
      append = FALSE)
}
