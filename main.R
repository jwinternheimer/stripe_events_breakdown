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
  
  # set the correct mrr amount
  mrr_changes <- get_mrr_change(event_types)
  
  
  return(events_discounted)
}


# get september events
# september <- main(start = '2017-09-01', end = '2017-09-30')
# saveRDS(september, file = 'sep.rds')

# get october events
# october <- main(start = '2017-10-01', end = Sys.Date())
# saveRDS(october, file = 'oct.rds')

