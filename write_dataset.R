get_dataset_queries <- function(DatasetId, access_token) {
  exec_q <- function(query) {
    qiverse.powerbi:::execute_rest_query_impl(DatasetId, query, access_token)
  }
  # Read DAX query from file and escape double quotes
  datasets_query <- gsub('"', '\\"', paste0(readLines("./datasets_query.msdax"), collapse="\n"),
                         fixed = TRUE)

  # A Streaming Dataset does not have a DAX query endpoint, so skip dataset if query fails
  table_definitions <- try(exec_q(datasets_query), silent = TRUE)
  if (inherits(table_definitions, "try-error")) {
    return(table_definitions)
  }

  list(table_definitions = table_definitions,
        relationships = exec_q("EVALUATE INFO.RELATIONSHIPS()"),
        measures = exec_q("EVALUATE INFO.MEASURES()"))
}

write_measures <- function(measures, dataset_path) {
  measures_path <- file.path(dataset_path, "Measures")
  if (nrow(measures) == 0) {
    if (dir.exists(measures_path)) {
      unlink(measures_path, recursive = TRUE)
    }
    return(invisible(NULL))
  }
  if (!dir.exists(measures_path)) {
    dir.create(measures_path, recursive = TRUE)
  }
  # If dataset has no measures in folders, then the DisplayFolder column is missing
  if (!("DisplayFolder" %in% colnames(measures))) {
    measures$DisplayFolder <- ""
  }
  measures <- measures[,c("Name", "DisplayFolder", "Expression")]

  # Need to replace characters that are not allowed in filenames before writing to file
  measures$DisplayFolder <- filename_clean(measures$DisplayFolder, folder = TRUE)
  measures$Name <- paste0(filename_clean(measures$Name), ".fs")
  # Need to explicitly replace NA with empty string otherwise an 'NA' folder will be created
  measures$DisplayFolder[is.na(measures$DisplayFolder)] <- ""

  curr_files <- list.files(measures_path, recursive = TRUE, pattern = ".fs")
  # When there is no DisplayFolder the path is listed with a // instead of a single /
  new_files <- gsub("//", "/", file.path(measures_path, measures$DisplayFolder, measures$Name),
                    fixed = TRUE)
  removed <- curr_files[!(file.path(measures_path, curr_files) %in% new_files)]

  if (length(removed) > 0) {
    purrr::walk(removed, function(rm_file) { unlink(file.path(measures_path, rm_file),
                                                    recursive = TRUE) })
  }

  purrr::pwalk(measures, function(Name, DisplayFolder, Expression) {
    file_path <- file.path(measures_path, DisplayFolder)
    if (!dir.exists(file_path)) {
      dir.create(file_path, recursive=TRUE)
    }
    cat(Expression, file = file.path(file_path, Name),
        sep = "\n", append = FALSE)
  })
}

write_erd <- function(table_definitions, relationships, dataset_path) {
  # Generate Mermaid table spec with column names and types for a given table
  # Mermaid Docs: https://mermaid.js.org/syntax/entityRelationshipDiagram.html#attributes
  table_details <- split(table_definitions, f = table_definitions$TableID) |>
    purrr::map_chr(function(table) {
      table <- dplyr::filter(table, !is.na(ColumnName))
      stringr::str_glue(
        "\"{dplyr::first(table$TableName)}\" {{",
        paste(stringr::str_c(table$ColumnTypeString, " ",
                              repl_invalid_chars(table$ColumnName)),
              collapse = "\n"),
        "}}",
        .sep = "\n"
      )
    })

  # Use DAX relationships table to generate Mermaid relationship spec
  # The relationship is labelled with the column names being linked, and the cardinality of relationship
  table_relationships <- ""
  if (nrow(relationships) > 0) {
    # DAX relationships table does not have column names for the To- and From- relationships (only IDs),
    # so need to join with table_definitions
    rels_named <- relationships |>
      dplyr::left_join(dplyr::select(table_definitions, FromTableID = TableID, FromTableName = TableName, FromColumnID = ColumnID, FromColumnName = ColumnName),
                        by = c("FromTableID", "FromColumnID")) |>
      dplyr::left_join(dplyr::select(table_definitions, ToTableID = TableID, ToTableName = TableName, ToColumnID = ColumnID, ToColumnName = ColumnName),
                        by = c("ToTableID", "ToColumnID")) |>
      dplyr::filter(!is.na(ToColumnName))

    table_relationships <- stringr::str_c(
      '"', rels_named$FromTableName, '" ',
      cardinal_map(rels_named),
      ' "', rels_named$ToTableName, '":"',
      repl_invalid_chars(rels_named$FromColumnName), '<br />',
      repl_invalid_chars(rels_named$ToColumnName),'"'
    )
    table_relationships <- table_relationships[!is.na(table_relationships)]
  }

  cat(paste(c("```mermaid", "erDiagram", table_details, table_relationships, "```"),
            collapse="\n"),
      file = file.path(dataset_path, "README.md"), append = FALSE)
}

write_queries <- function(table_definitions, dataset_path) {
  curr_files <- list.files(path = dataset_path, pattern = ".fs")
  removed <- curr_files[!(curr_files %in% paste0(table_definitions$TableName, ".fs"))]
  if (length(removed) > 0) {
    purrr::walk(removed, function(rm_file) { unlink(file.path(dataset_path, rm_file),
                                                    recursive = TRUE) })
  }

  table_definitions |>
    dplyr::select(TableName, QueryDefinition) |>
    dplyr::distinct() |>
    purrr::pwalk(function(TableName, QueryDefinition) {
      cat(QueryDefinition, file = file.path(dataset_path, paste0(TableName, ".fs")),
          sep = "\n", append = FALSE)
    })
}

write_dataset <- function(Dataset, DatasetId, Workspace, access_token) {
  message("  Ds: ", Dataset)
  dataset_metadata <- get_dataset_queries(DatasetId, access_token)
  if (inherits(dataset_metadata, "try-error")) {
    return(invisible(NULL))
  }
  dataset_path <- file.path(Workspace, "Datasets", Dataset)
  if (!dir.exists(dataset_path)) {
    dir.create(dataset_path, recursive = TRUE)
  }
  write_queries(dataset_metadata$table_definitions, dataset_path)
  write_measures(dataset_metadata$measures, dataset_path)
  write_erd(dataset_metadata$table_definitions, dataset_metadata$relationships, dataset_path)
}
