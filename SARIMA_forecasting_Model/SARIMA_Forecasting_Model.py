# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 21:21:09 2023

@author: jcgarcia
"""

import pandas as pd
from datetime import datetime, timedelta
import matplotlib.dates as mdates
from datetime import datetime
import statsmodels.api as sm
from pmdarima.arima import auto_arima
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from matplotlib.ticker import FuncFormatter
import calendar
from statsmodels.tsa.api import Holt



# Importar los datos desde el archivo Excel
archivo_excel = r"C:\Users\jcgarcia\OneDrive - NCR Financial Services\Escritorio\Projections_1.xlsx"
aba = pd.read_excel(archivo_excel)

aba['Date'] = pd.to_datetime(aba['Date'])
aba.set_index('Date', inplace=True)
aba.index = pd.date_range(start=aba.index[0], periods=len(aba), freq='D')


model_holt = Holt(aba['Connected']).fit(optimized=True)
aba['Holt'] =model_holt.fittedvalues

# Calcule los cuantiles 1% y 99%
quantile_1 = aba['Connected'].quantile(0.01)
quantile_99 = aba['Connected'].quantile(0.99)
aba['Connected'] = aba['Connected'].clip(quantile_1, quantile_99)

print(aba.tail())
print(aba.shape)

#------------------------------------ Crear variables dummy para los días de la semana
dow = pd.get_dummies(aba['Dow'])

# Ajustar las variables dummy
dow['sábado'] = dow['sábado'] * 2

# Concatenar las variables dummy con el DataFrame original
aba_with_dummies = pd.concat([aba, dow], axis=1)
aba_with_dummies['domingo'] = 0

print(aba_with_dummies)
#-------------------------------Analisis EDA  de la serie temporal -----------------------------------------
#Descomposciion de la serie temporal
result = seasonal_decompose(aba['Connected'], model='additive')
result.plot()
plt.show()

# prueba ADF de estacionalidad
result = adfuller(aba['Connected'])
print(f'Estadístico ADF: {result[0]}')
print(f'Valor p: {result[1]}')

# Gráficos de ACF y PACF
plot_acf(aba['Connected'])
plot_pacf(aba['Connected'])
plt.show()

#---------------------------SARIMA con variable hexogena dia de la semana -------------------------------------------------------
#Identificar componentes P, Q y d
autoarima_model = auto_arima(aba['Connected'], seasonal= True, stepwise=True, trace=True)
#best_p, best_d, best_q = autoarima_model.get_params()['order']

#Asignando variable hexogena con dia de la semana 
print(dow.columns)

exog_var= aba_with_dummies[['jueves', 'lunes', 'martes', 'miércoles',
       'sábado', 'viernes']]

# Ajusta el modelo SARIMAX con aumento de maxiter
model = sm.tsa.SARIMAX(aba['Connected'], exog=exog_var, order=(4, 1,3), seasonal_order=(1, 1, 1, 7))
results = model.fit(maxiter=1000)  # Aumentar el número máximo de iteraciones
print('Modelo ajustado correctamente')


# si el forecast a crear es semanal poner "semanal" si es mensual "mensual" si es trimestral "anual"
frecuencia_pronostico = "mensual"

if frecuencia_pronostico == "semanal":
    forecast_steps = 7
elif frecuencia_pronostico == "mensual":
    forecast_steps = 31
elif frecuencia_pronostico == "trimestral":
       forecast_steps = 90 
elif frecuencia_pronostico == "anual":
    forecast_steps = 365
else:
    print("Frecuencia de pronóstico no válida")

#-------------------------------------------- Crear DataFrame Pronostico
pronostico = pd.DataFrame(columns=['Date', 'DOW', '%_Projected'])
ultima_fecha = aba_with_dummies.index[-1]
primera_fecha = ultima_fecha + timedelta(days=1)
dias_semana_espanol = list(calendar.day_name)

#Ingestar datos en dataframe pronostico
for i in range(forecast_steps):
    # Calcular la fecha para cada fila consecutivamente
    siguiente_fecha = primera_fecha + timedelta(days=i)
    nombre_dia_semana = dias_semana_espanol[siguiente_fecha.weekday()]
    
    # Crear un DataFrame temporal para cada fila
    fila_temporal = pd.DataFrame({
        'Date': [siguiente_fecha],
        'DOW': [nombre_dia_semana],
        'Conected_Projected': [0]
    })
    pronostico = pd.concat([pronostico, fila_temporal], ignore_index=True)
print(pronostico)
#Filtrar dataframe para eliminar el domingo 
#pronostico_filtrado = pronostico.loc[pronostico['DOW'] != 'Sunday']

# Obtener variables dummy de la columna 'DOW' en el DataFrame 'pronostico'
forecast_exog = pd.get_dummies(pronostico['DOW'])
forecast_exog['domingo'] = 0

# Selecciona solo las columnas deseadas del DataFrame forecast_exog
forecast_exog = pd.get_dummies(pronostico['DOW'])[['Friday', 'Monday', 'Saturday', 'Thursday', 'Tuesday', 'Wednesday']]


# Calcular el forecast requerido
forecast = results.get_forecast(steps=forecast_steps, exog=forecast_exog)
forecast_mean = forecast.predicted_mean
forecast_mean[forecast_mean < 0] = 0
forecast_ci = forecast.conf_int()
forecast_ci.iloc[:, 0] = forecast_mean

forecast_mean_list = forecast_mean.tolist()
#forecast_mean_list_enteros = [round(valor) for valor in forecast_mean_list]
print(forecast_mean_list)

#Agregar el valor proyectado al dataframe pronostico
pronostico['Conected_Projected'] = forecast_mean_list
print(pronostico)

#---------------------------Validaciones finales -------------------------------------------------------

# Filtrar los datos de los últimos 3 meses en 'aba'
fecha_inicio = pronostico['Date'].iloc[0] - pd.DateOffset(months=3)
datos_ultimos_3_meses = aba['%'].loc[fecha_inicio:]


fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(datos_ultimos_3_meses.index, datos_ultimos_3_meses, label='Conected (Datos reales)', color='blue')
ax.plot(pronostico['Date'], pronostico['Conected_Projected'], label='Conected_Projected (Proyecciones)', color='red')
ax.set_title('Últimos 3 Meses: Datos Reales vs. Proyecciones')
ax.set_xlabel('Fecha')
ax.set_ylabel('Valor')
ax.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

#-------------------------- graficar la columna %-----------------------------------------
# Plot the actual data
plt.figure(figsize=(12, 6))
plt.plot(aba['%'], label='Holt-Winters', linestyle='--', color='red')
plt.title('Recorte de Outliers con rango intercuartil')
plt.xlabel('Date')
plt.ylabel('Percentage')
plt.legend()
plt.grid(True)
plt.show()




