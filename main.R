source('helper_functions.R')

main <- function(start, end) {
  
  # get subscriptions and charges
  subs <- get_subscriptions()
  
  # get total number of charges for subscription
  subs_total_charges <- subs %>%
    group_by(subscription_id) %>%
    summarise(total_charges = sum(charges))
  
  # join total number of charges
  subs <- subs %>%
    left_join(subs_total_charges, by = 'subscription_id')
  
  # get discount events
  discounts <- get_discount_events()
  
  # parse json data in discounts
  discounts_parsed <- parse_discount_json(discounts)
  
  # clean discounts
  discounts_cleaned <- clean_discounts(discounts_parsed)
  
  # group by customer
  customer_discounts <- discounts_cleaned %>%
    group_by(customer_id) %>%
    summarise(discount_amount_off = max(discount_amount_off), discount_percent_off = max(discount_percent_off),
              discount_start_date = min(discount_start_date), discount_end_date = max(discount_end_date))
  
  # get stripe events
  events <- get_stripe_events(start_date = start, end_date = end)
  
  # parse json
  events_parsed <- parse_json(events)
  
  # join subscription data
  sub_events <- events_parsed %>%
    left_join(subs, by = c('subscription_id' = 'subscription_id', 'plan_id' = 'plan_id')) %>%
    left_join(subs, by = c('subscription_id' = 'subscription_id', 'previous_plan_id' = 'plan_id'),
              suffix = c("", "_for_previous_plan"))
  
  # get mrr amounts
  mrr_events <- get_mrr_amounts(sub_events)
  
  # get event types
  event_types <- get_event_types(mrr_events)
  
  # set the correct mrr amount
  mrr_changes <- get_mrr_change(event_types)
  
  # join discounts
  events_with_discounts <- mrr_changes %>%
    left_join(customer_discounts, by = 'customer_id')
  
  # apply discounts
  events_discounted <- apply_discount(events_with_discounts)
  
  events_discounted$discounted_amount[is.na(events_discounted$discounted_amount)] <- 0
  
  events_discounted <- events_discounted %>%
    mutate(mrr_amount = mrr_amount - discounted_amount)
  
  # remove duplicates
  #events_discounted <- events_discounted %>%
  #  group_by(id) %>%
  #  arrange(mrr_amount) %>%
  #  filter(row_number() == 1) %>%
  #  ungroup()
  
  return(mrr_changes)
}



# set start date
start <- '2017-09-01'
end <- '2017-09-30'

# get september events
september <- main(start, end)

# get august events
august <- main(start = '2017-08-01', end = '2017-08-31')

# sum events
september %>%
  filter(event_type != 'other') %>%
  group_by(event_type) %>%
  summarise(mrr = sum(mrr_amount))

# sum events
august %>%
  filter(event_type != 'other') %>%
  group_by(event_type) %>%
  summarise(mrr = sum(mrr_amount))




# read data
april <- readRDS('april_events.rds')
may <- readRDS('may_events.rds')
june <- readRDS('june.rds')
july <- readRDS('july_events.rds')
august <- readRDS('august_events.rds')
september <- readRDS('september_events.rds')
october <- readRDS('october_events.rds')

# bind events
all_events <- rbind(april, may, june, july, august, september, october)
events_parsed <- all_events

# save
saveRDS(events_discounted, file = '~/Documents/stripe-mrr-breakdown/april_oct.rds')
