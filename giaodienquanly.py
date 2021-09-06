# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import streamlit as st
import pandas as pd
#from datetime import date
from io import BytesIO
import requests
import plotly.express as px

regex = {'MSBN': st.secrets['msbn'], 'MSBS':st.secrets['msbs']}

#dmin = date.fromisoformat('2021-08-08')
#dmax = date.today()

# BN dataset
ncol_1 = range(4)
skip_1 = 7
msbn_1 = 2
msbs_1 = 3


# BS dataset
ncol_2 = range(4)
skip_2 = 0
msbn_2 = 2
msbs_2 = 1

end_status = ['Hết bệnh', 'Tử vong', 'Ngưng theo dõi']


#-----------------------------------------------------

def Read_DF(url, ncol, skip, bn_col, bs_col):
    spreadsheet_id = url.split('/')[-2]
    file_name = 'https://docs.google.com/spreadsheets/d/{}/export?format=xlsx'.format(spreadsheet_id)
    r = requests.get(file_name)
    df = pd.read_excel(BytesIO(r.content), usecols=ncol, skiprows=skip, dtype={bn_col:'str', bs_col:'str', 0:'datetime64'})
    df.rename(columns={df.columns[bn_col]:'MSBN',  df.columns[bs_col]:'MSBS', df.columns[0]:'Timestamp'}, inplace=True)
    for i in ['MSBN', 'MSBS']:
        df[i] = df[i].str.upper().str.replace(' ','')
    return df

def Count_MSBS(df1,df2):
    df = df1[['MSBN','MSBS']].merge(df2[['MSBN','MSBS']], how='outer')
    df['MSBS_error'] = df['MSBS'].where(~df['MSBS'].str.match(regex['MSBS']), other=float('NaN'))
    bn_bs = df.groupby('MSBN').agg({'MSBS':['unique','nunique'],'MSBS_error':['unique','nunique']})
    bn_bs.columns = ['MSBS','n1','MSBS_error', 'n2']
    bn_bs['score']= bn_bs['n1'] - bn_bs['n2']
    return bn_bs.drop(columns=['n1','n2']).sort_values('score').sort_index()

def Count_MSBN(df1,df2):
    df_total = pd.concat([df1[['Timestamp','MSBN']],df2[['Timestamp','MSBN']]])
    df_first = df_total.groupby('MSBN')['Timestamp'].min().reset_index()
    report = {}
    report['Tất cả'] = df_total.groupby(df_total['Timestamp'].dt.date)['MSBN'].nunique()
    report['Ca mới'] = df_first.groupby(df_first['Timestamp'].dt.date)['MSBN'].nunique()
    report['Ca tự theo dõi'] = df1.groupby(df1['Timestamp'].dt.date)['MSBN'].nunique()
    report['Ca được nhận/xử trí'] = df2.groupby(df2['Timestamp'].dt.date)['MSBN'].nunique()
    return pd.DataFrame(report).reset_index()

def Load_Data(path1, path2):
    df1 = Read_DF(path1, ncol_1, skip_1, msbn_1, msbs_1).drop(columns='Score').sort_values('Timestamp', ascending=False)
    df1_error = df1[~df1['MSBN'].str.match(regex['MSBN'])]

    df2 = Read_DF(path2, ncol_2, skip_2, msbn_2, msbs_2).sort_values('Timestamp', ascending=False)
    df2_error = df2[~df2['MSBN'].str.match(regex['MSBN'])].drop(columns='Xử trí trong ngày')

    bn_bs = Count_MSBS(df1,df2)
    overview = Count_MSBN(df1,df2)

    return df1_error, df2_error, bn_bs, overview

def Show_DF(df):
    df = df.set_index(df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S'))
    df_style = df.drop(columns='Timestamp').style
    return df_style.apply(lambda x: ['background: yellow' * len(x)], subset='MSBN', axis=1)

#-------------------------------------------------------------------------#

st.title('PKNM - Giao diện quản lý')

path1 = st.secrets['path1']
path2 = st.secrets['path2']

df1_error, df2_error, bn_bs, overview = Load_Data(path1, path2)

#-------------
st.header('Kiểm tra MSBN lỗi')
col1, col2 = st.beta_columns(2)
with col1:
    st.subheader('BNTuDanhGia: {} cập nhật lỗi'.format(len(df1_error)))
    if len(df1_error)>0:
        st.table(Show_DF(df1_error))

with col2:
    st.subheader('PhieuXuTri: {} cập nhật lỗi'.format(len(df2_error)))
    if len(df2_error)>0:
        st.table(Show_DF(df2_error))

#-------------
st.header('Kiểm tra MSBS tương ứng')
col3, col4 = st.beta_columns(2)
bn_bs_miss = bn_bs[bn_bs['score']<1]
with col3:
    st.write('{} MSBN không có MSBS hợp quy cách'.format(len(bn_bs_miss)))
with col4:
    extra = st.checkbox('Xem tất cả')
if extra:
    st.table(bn_bs.drop(columns='score'))
elif len(bn_bs_miss)>0:
    st.table(bn_bs_miss.drop(columns='score'))

#--------
st.header('Xem xét số ca trong ngày')
fig = px.line(overview.melt(id_vars='Timestamp'), x='Timestamp', color='variable', y='value')
st.plotly_chart(fig)
