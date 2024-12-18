WITH monthly_revenue AS (
    SELECT 
        gpu.user_id,
        gpu.language,
        gpu.age,
        gpu.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date)) AS payment_month,
        SUM(gp.revenue_amount_usd) AS total_revenue
    FROM project.games_paid_users gpu
    LEFT JOIN project.games_payments gp ON gpu.user_id = gp.user_id
    GROUP BY gpu.user_id, gpu.language, gpu.age, gpu.game_name, DATE_TRUNC('month', gp.payment_date)
)
,revenue_lag_lead_months as (
	select
		*,
		date(payment_month - interval '1' month) as previous_calendar_month,
		date(payment_month + interval '1' month) as next_calendar_month,
		lag(total_revenue) over(partition by user_id order by payment_month) as previous_paid_month_revenue,
		lag(payment_month) over(partition by user_id order by payment_month) as previous_paid_month,
		lead(payment_month) over(partition by user_id order by payment_month) as next_paid_month
	from monthly_revenue
)
, revenue_metrics AS (
    SELECT 
        rl.*,
        case 
			when previous_paid_month is null 
				then total_revenue
		end as new_mrr,
		case 
			when previous_paid_month != previous_calendar_month 
				and previous_paid_month is not null
				then total_revenue
		end as back_from_churn_revenue,
        case 
			when next_paid_month is null 
			or next_paid_month != next_calendar_month
				then total_revenue
		end as churned_revenue,
        case 
			when previous_paid_month = previous_calendar_month 
				and total_revenue > previous_paid_month_revenue 
				then total_revenue - previous_paid_month_revenue
		end as expansion_revenue,
		case 
			when previous_paid_month = previous_calendar_month 
				and total_revenue < previous_paid_month_revenue 
				then total_revenue - previous_paid_month_revenue
		end as contraction_revenue,
		CASE 
    		WHEN next_paid_month IS NULL 
         		OR next_paid_month != next_calendar_month THEN payment_month
		END AS churn_month
    FROM revenue_lag_lead_months rl
)
SELECT 
    rm.payment_month,
    rm.user_id,
    rm.language,
    rm.age,
    rm.game_name,
    rm.total_revenue,
    rm.new_mrr,
    rm.back_from_churn_revenue,
    rm.churned_revenue,
    rm.expansion_revenue,
    rm.contraction_revenue,
    rm.churn_month
FROM revenue_metrics rm
ORDER BY rm.payment_month, rm.user_id;