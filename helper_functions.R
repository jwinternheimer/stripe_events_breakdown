# load libraries
library(buffer); library(dplyr); library(tidyr); library(jsonlite); library(lubridate)


# define function to get all stripe subscriptions and successful charges
get_subscriptions <- function() {
  
  # define query to get subscriptions
  subscription_query <- "
  select
    s.id as subscription_id
    , i.subscription_plan_id as plan_id
    , count(distinct c.id) as charges
  from stripe._subscriptions as s
  join stripe._invoices as i
    on s.id = i.subscription_id
  join stripe._charges as c
    on c.id = i.charge
  where c.captured and i.paid
  group by 1, 2
  "
  
  # connect to redshift
  con <- redshift_connect()
  
  # run query
  subs <- query_db(subscription_query, con)
  
  # get total number of charges for subscription
  subs_total_charges <- subs %>%
    group_by(subscription_id) %>%
    summarise(total_charges = sum(charges))
  
  # join total number of charges
  subs <- subs %>%
    left_join(subs_total_charges, by = 'subscription_id')
  
  # retun subscriptions
  return(subs)
}


# define function to get stripe events
get_stripe_events <- function(start_date, end_date) {
  
  # connect to redshift
  con <- redshift_connect()
  
  # define query to get stripe events
  events_query <- sprintf("
                          select *
                          from stripe_api_events
                          where type in (
                            'customer.subscription.created', 
                            'customer.subscription.deleted', 
                            'customer.subscription.updated'
                          )
                          and date(date) >= date(\'%s\') and date(date) <= date(\'%s\')
                          "
                          , start_date, end_date)
  
  # run query
  events <- query_db(events_query, con) %>%
    select(date:id, type)
  
  # return events
  return(events)
}


# define function to gather discount events
get_discount_events <- function() {
  
  # connect to redshift
  con <- redshift_connect()
  
  # define query to get stripe events
  events_query <- "
    select *
    from stripe_api_events
    where type in ('customer.discount.created', 'customer.discount.deleted', 'customer.discount.updated')
  "
  
  # run query
  events <- query_db(events_query, con) %>%
    select(date:id, type)
  
  # return events
  return(events)
}

# define function to parse json from discount data
parse_discounts <- function(discounts) {
  
  # parse json and create a data.frame
  res <- jsonlite::stream_in(textConnection(discounts$data))
  
  # get the key information
  discounts <- discounts %>%
    mutate(discount_id = res$object$id, 
           customer_id = res$object$customer,
           discount_duration = res$object$coupon$duration,
           discount_duration_in_months = res$object$coupon$duration_in_months,
           discount_amount_off = res$object$coupon$amount_off,
           discount_percent_off = res$object$coupon$percent_off,
           discount_start_date = res$object$start,
           discount_end_date = res$object$end) %>%
    mutate(discount_start_date = as.POSIXct(discount_start_date, origin = "1970-01-01"),
           discount_end_date = as.POSIXct(discount_end_date, origin = "1970-01-01"))
  
  # return data frame
  return(discounts)
}

# define function that cleans discounts
clean_discounts <- function(discounts) {
  
  # group by id and get dates
  discounts_cleaned <- discounts %>%
    mutate(discount_amount_off = discount_amount_off / 100) %>%
    group_by(discount_id, customer_id, discount_amount_off, discount_percent_off) %>%
    summarise(discount_start_date = min(discount_start_date, na.rm = TRUE), 
              discount_end_date = max(discount_end_date, na.rm = TRUE))
  
  # group by customer
  customer_discounts <- discounts_cleaned %>%
    group_by(customer_id) %>%
    summarise(discount_amount_off = max(discount_amount_off), discount_percent_off = max(discount_percent_off),
              discount_start_date = min(discount_start_date), discount_end_date = max(discount_end_date))
  
  # return discounts
  return(customer_discounts)
}

# define function to get discounts
get_discounts <- function() {
  
  discounts <- get_discount_events()
  discounts_parsed <- parse_discounts(discounts)
  discounts_cleaned <- clean_discounts(discounts_parsed)
  
  return(discounts_cleaned)
}

# define function to parse json and return specific fields
parse_json <- function(df) {
  
  # parse json and create a data.frame
  res <- jsonlite::stream_in(textConnection(df$data))
  
  # get the key information
  df <- df %>%
    mutate(subscription_id = res$object$id, 
           customer_id = res$object$customer,
           status = res$object$status,
           plan_id = res$object$plan$id,
           plan_amount = res$object$plan$amount,
           plan_interval = res$object$plan$interval,
           previous_plan_id = res$previous_attributes$plan$id,
           previous_plan_amount = res$previous_attributes$plan$amount,
           previous_plan_interval = res$previous_attributes$plan$interval,
           previous_status = res$previous_attributes$status)
  
  # return data frame
  return(df)
}

# function to get parsed subscription events
get_subscription_events <- function(start, end) {
  
  # get stripe events
  print('collecting events')
  events <- get_stripe_events(start_date = start, end_date = end)
  
  # parse json
  print('parsing json')
  events_parsed <- parse_json(events)
  print('done')
  
  # return subscription events
  return(events_parsed)
}


# define function to get mrr amounts
get_mrr_amounts <- function(df) {
  
  # calculate monthly value of plan
  df <- df %>%
    mutate(plan_amount = plan_amount / 100, previous_plan_amount = previous_plan_amount / 100) %>%
    mutate(plan_mrr_amount = ifelse(plan_interval == 'year', plan_amount / 12, plan_amount),
           previous_plan_mrr_amount = ifelse(previous_plan_interval == 'year', previous_plan_amount / 12,
                                             previous_plan_amount))
  
  # return dataframe
  return(df)
}

# define function to apply discounts
apply_discount <- function(df) {
  
  # determine if discount applies
  df <- df %>%
    mutate(discount_applies = (discount_start_date < (date + days(4)) & (is.na(as.Date(discount_end_date, '%Y-%m-%d', tz = 'UTC')) | discount_end_date > date))) %>%
    mutate(plan_discount_amount = ifelse(discount_applies, plan_mrr_amount * (discount_percent_off / 100), 0),
           previous_plan_discount_amount = ifelse(discount_applies, previous_plan_mrr_amount * (discount_percent_off / 100), 0))
  
  # replace na with 0
  df$plan_discount_amount[is.na(df$plan_discount_amount)] <- 0
  df$previous_plan_discount_amount[is.na(df$previous_plan_discount_amount)] <- 0
  
  # subtract discounts from MRR
  df <- df %>%
    mutate(plan_mrr_amount = plan_mrr_amount - plan_discount_amount,
           previous_plan_mrr_amount = previous_plan_mrr_amount - previous_plan_discount_amount)
  
  # return df
  return(df)
}

# define the event types
get_event_types <- function(df) {
  
  # set event types
  df <- df %>%
    mutate(event_type = ifelse(type == 'customer.subscription.deleted' & !is.na(charges), 'churn',
                        ifelse(type == 'customer.subscription.created' & status == 'active' & !is.na(charges), 'new',
                        ifelse(status == 'active' & previous_status == 'trialing' &
                                !is.na(charges) & is.na(charges_for_previous_plan), 'trial_conversion', 
                        ifelse(status == 'active' & plan_mrr_amount > previous_plan_mrr_amount & !is.na(charges) & !is.na(charges_for_previous_plan), 'upgrade',
                        ifelse(status == 'active' & plan_mrr_amount < previous_plan_mrr_amount & !is.na(charges) & !is.na(charges_for_previous_plan), 'downgrade',
                        'other'))))))
  
  
  return(df)
}

# get mrr change amounts
get_mrr_change <- function(df) {
  
  # handle upgrades and downgrades
  df <- df %>%
    mutate(mrr_amount = ifelse(event_type == 'upgrade' | event_type == 'downgrade', plan_mrr_amount - previous_plan_mrr_amount,
                               ifelse(event_type == 'churn', plan_mrr_amount * -1, plan_mrr_amount)))
  
  return(df)
}
