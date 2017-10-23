source('helper_functions.R')

main <- function(start, end) {
  
  # get subscriptions and charges
  subs <- get_subscriptions()
  
  # get subscription events
  sub_events <- get_subscription_events(start, end)
  
  # join subscription data
  sub_events <- sub_events %>%
    left_join(subs, by = c('subscription_id' = 'subscription_id', 'plan_id' = 'plan_id')) %>%
    left_join(subs, by = c('subscription_id' = 'subscription_id', 'previous_plan_id' = 'plan_id'),
              suffix = c("", "_for_previous_plan"))
  
  # get discount events
  discounts <- get_discounts()
  
  # get mrr amounts
  mrr_events <- get_mrr_amounts(sub_events)
  
  # get event types
  event_types <- get_event_types(mrr_events)
  
  # join discounts
  events_with_discounts <- event_types %>%
    left_join(discounts, by = 'customer_id')
  
  # apply discounts
  events_discounted <- apply_discount(events_with_discounts)
  
  # get upgrades and downgrades (we'll call them upgrades)
  upgrades <- events_discounted %>%
    filter(event_type == 'upgrade' | event_type == 'downgrade')
  
  # duplicate upgrade events
  upgrade_dups <- upgrades
  
  # set event types and IDs
  upgrades <- upgrades %>%
    mutate(event_type = paste(event_type, 'in', sep = '_')) %>%
    mutate(id = paste(id, event_type, sep = '_'))
  
  upgrade_dups <- upgrade_dups %>%
    mutate(event_type = paste(event_type, 'out', sep = '_')) %>%
    mutate(id = paste(id, event_type, sep = '_'))
  
  # switch plan id and plan mrr amount for upgrades out
  upgrade_dups <- upgrade_dups %>%
    mutate(plan_id = previous_plan_id,
           plan_mrr_amount = previous_plan_mrr_amount * -1)
  
  # merge upgrades in and out
  upgrades_in_and_out <- rbind(upgrades, upgrade_dups) 
  
  # merge into original data frame
  final_events <- events_discounted %>%
    filter(event_type != 'upgrade' & event_type != 'downgrade') %>%
    bind_rows(upgrades_in_and_out) %>%
    mutate(plan_mrr_amount = ifelse(event_type == 'churn', plan_mrr_amount * -1, plan_mrr_amount))
  
  # return final data frame
  return(final_events)
}


# get september events
# september <- main(start = '2017-09-01', end = '2017-09-30')
# saveRDS(september, file = 'sep.rds')

# get october events
# october <- main(start = '2017-10-01', end = Sys.Date())
# saveRDS(october, file = 'oct.rds')

