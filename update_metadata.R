
future::plan(future::multisession())
furrr::future_walk(seq(1, 10), function(i) {
  message("i: ", i)
}, .options = furrr::furrr_options(scheduling = Inf))
future::plan(future::sequential())
