-- net = gross - discount y gross = quantity * unit_price (tolerancia por redondeo)
select order_line_key, quantity, unit_price, gross_amount, discount_amount, net_amount
from {{ ref('fact_sales') }}
where abs(gross_amount - discount_amount - net_amount) > 0.01
   or abs(quantity * unit_price - gross_amount) > 0.01
   or net_amount < 0
