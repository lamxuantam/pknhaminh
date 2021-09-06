# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import streamlit as st
import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
from io import BytesIO
import requests

demo = {
    'Cấp cứu': 'Có triệu chứng cấp cứu',
    'tăng'  : 'Triệu chứng tăng nặng',
    'mới'   : 'Triệu chứng mới xuất hiện',
    'ổn-giảm'  : 'Triệu chứng ổn định hoặc giảm nhẹ',
    'Có'    : 'Có triệu chứng khác hoặc cần hỗ trợ',
    'MSBN'  : 'Bệnh nhân chưa có đánh giá theo dõi gần nhất từ bác sĩ',
    '    '  : 'Đánh giá theo dõi của bác sĩ'
}

msbs_admin = st.secrets['master_code']

noneed = ['Hỗ trợ', '(vui lòng ghi rõ)']
end_status = ['Hết bệnh', 'Tử vong', 'Ngưng theo dõi']

ncol_1 = range(19)
skip_1 = 7
msbn_1 = 2
msbs_1 = 3
rename_1 = {
    0: 'Timestamp',
    2: 'MSBN',
    3: 'MSBS_main',
    17: 'Triệu chứng khác',
    18: 'Cần hỗ trợ'
}

ncol_2 = range(5)
skip_2 = 0
msbn_2 = 2
msbs_2 = 1
rename_2 = {
    0: 'Timestamp',
    2: 'MSBN',
    1: 'MSBS',
}


convert = {
    'Có': 'Cấp cứu',
    'CÓ, tăng nặng': 'tăng',
    'CÓ, mới xuất hiện': 'mới',
    'CÓ, ổn định hoặc giảm nhẹ': 'ổn-giảm',
    'Không, hiện tôi cảm thấy ổn': '',
    'KHÔNG': '',
    'Không': ''
}

color = {
    'Cấp cứu': 'background: red',
    'tăng'  : 'background: orange',
    'mới'   : 'background: yellow',
    'ổn-giảm'  : 'background: green',
    'Có'    : 'background: gray',
    'MSBN'  : 'background: cyan',
    '    '  : 'background: skyblue'
}

rate = {
    'Cấp cứu': 1000,
    'tăng'  : 100,
    'mới'   : 10,
    'ổn-giảm'  : 1,
    'Có'    : 1
}
#-----------------------------------------------------

def Read_DF(url, ncol, skip, rename, bn_col, bs_col):
    spreadsheet_id = url.split('/')[-2]
    file_name = 'https://docs.google.com/spreadsheets/d/{}/export?format=xlsx'.format(spreadsheet_id)
    r = requests.get(file_name)
    df = pd.read_excel(BytesIO(r.content), usecols=ncol, skiprows=skip, dtype={bn_col:'str', bs_col:'str', 0:'datetime64'})
    for i in [bn_col, bs_col]:
        df[df.columns[i]] = df[df.columns[i]].str.upper().str.replace(' ','')
    df.rename(columns={df.columns[i]:rename[i] for i in rename}, inplace=True)
    return df.fillna('')

# def Read_DF(path, ncol, skip, rename, bn_col, bs_col):
#     df = pd.read_excel(path, usecols=ncol, skiprows=skip, dtype={bn_col:'str', bs_col:'str', 0:'datetime64'})
#     df.rename(columns={df.columns[i]:rename[i] for i in rename}, inplace=True)
#     return df

def Remove_Extra(s):
    for subs in noneed:
        try:
            s= s.replace(subs, '')
        except:
            s=s
    return s

def Create_MainDF (df1, df2):
    df2 = df2.assign(idx=np.arange(df2.shape[0]))
    df2['Xử lý'] = df2['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df = pd.merge_asof(
        df1.sort_values('Timestamp'), df2[['MSBN','Timestamp','idx']].sort_values('Timestamp'),
        by='MSBN', on= 'Timestamp', direction='forward'
    ).merge(df2, on=['MSBN','idx'], how='outer').assign(
        Timestamp=lambda x: x['Timestamp_x'].fillna(x['Timestamp_y'])
    ).drop(['Timestamp_x','Timestamp_y','idx'], axis=1).sort_values('Timestamp', ascending=False)

    
    df['date'] = df['Timestamp'].dt.strftime('%Y-%m-%d').apply(lambda x: date.fromisoformat(x))

    return df


def Create_ShortDF (df, shorten_cols, rate_cols):
    short_df = df.drop_duplicates('MSBN').copy()
    for col in shorten_cols:
        short_df[col] = short_df[col].where((short_df[col].isnull()) | (short_df[col]==''), other='Có')
    short_df['rate'] = short_df[rate_cols].apply(lambda x: sum([rate[i] if i in rate else 0 for i in x]), axis=1)
    short_df['rate'] = short_df['rate'].where(~short_df['Xử lý'].isnull(), other=short_df['rate']+1000)
    return short_df

def Create_TextDF (df1, df2, df1_cols, df2_cols):
    df1['Note'] = df1.apply(lambda x: ', '.join([col.upper()+' '+str(x[col]) for col in df1_cols if x[col]!='']),axis=1)
    df2['Note'] = df2.apply(lambda x: ', '.join([x[col] for col in df2_cols if x[col]!='']),axis=1)
    text_df = df1.merge(df2, on=['Timestamp', 'MSBN','Note'], how='outer')
    text_df['date'] = text_df['Timestamp'].dt.strftime('%Y-%m-%d').apply(lambda x: date.fromisoformat(x))

    return text_df[['Timestamp', 'MSBN','Note', 'MSBS','date']].sort_values('Timestamp', ascending=False)

@st.cache(ttl=300)
def Load_Data(path1, path2):
    df1 = Read_DF(path1, ncol_1, skip_1, rename_1, msbn_1, msbs_1).drop(columns='Score')
    df2 = Read_DF(path2, ncol_2, skip_2, rename_2, msbn_2, msbs_2)

    bn_cols = df1.columns[range(3,18)]
    text_cols = df1.columns[[16, 17]]
    bs_cols = df2.columns[[3,4]]
    status_col = df2.columns[3]

    for col in bn_cols:
        df1[col] = df1[col].apply(lambda x: convert[x] if x in convert else x)
    df1['Cần hỗ trợ'] = df1['Cần hỗ trợ'].apply(lambda x: Remove_Extra(x))

    main_df = Create_MainDF(df1, df2)

    short_df = Create_ShortDF(main_df, text_cols, bn_cols)

    text_df = Create_TextDF(df1, df2, bn_cols, bs_cols)

    total_close = short_df[short_df[status_col].isin(end_status)]['MSBN'].unique().tolist()
    total_open = list(set(short_df['MSBN'].unique()) - set(total_close))
    
    dsbs = list(set(main_df['MSBS'].dropna()).union(set(main_df['MSBS_main'].dropna())))

    date = [main_df['date'].min(),main_df['date'].max()]

    dup_index = main_df.index[main_df.duplicated(subset=[*bs_cols,*['Xử lý']])]
    main_df.loc[dup_index, bs_cols] = ''

    return main_df.fillna(''), short_df.fillna(''), text_df.fillna(''), bn_cols, bs_cols, total_close, total_open, dsbs, date


def Filter_Patients(msbs):
    if msbs == msbs_admin:
        bn_open = total_open
        bn_close = total_close
    else:
        dsbn = main_df[(main_df['MSBS']==msbs) | (main_df['MSBS_main']==msbs)]['MSBN'].unique()
        bn_open = list(set(dsbn) & set(total_open))
        bn_close = list(set(dsbn) & set(total_close))

    return bn_open, bn_close


def Show_DF (df, timerange, color1, color2=[], drop_cols=[], waiting=False):
    sub_df = df[(df['date']>=timerange[0]) & (df['date']<=timerange[1])]
    sub_df = sub_df.set_index(sub_df['Timestamp'].dt.strftime('%d/%m/%Y %H:%M:%S')).drop(columns=[*['Timestamp','date'], *drop_cols])
    df_style = sub_df.style
    df_style.apply(lambda x: [color[i] if i in color else '' for i in x], subset=color1)
    df_style.apply(lambda x: ['font-weight: bold' * len(x)], subset='MSBN', axis=1)
    for col in color2:
        df_style.apply(lambda x: ['background: skyblue' * len(x)], subset=col, axis=1)
    
    if waiting:
        df_style.apply(lambda x: ['background: cyan' if i in sub_df[sub_df['Xử lý']=='']['MSBN'].unique().tolist() else '' for i in x], subset='MSBN')
         
    return df_style

def Show_TextDF (df, timerange):
    sub_df = df[(df['date']>=timerange[0]) & (df['date']<=timerange[1])]
    sub_df = sub_df.set_index(sub_df['Timestamp'].dt.strftime('%d/%m/%Y %H:%M:%S')).drop(columns=['Timestamp','date'])
    df_style = sub_df.style
    df_style.apply(lambda x: ['font-weight: bold' * len(x)], subset='MSBN', axis=1)
    df_style.apply(lambda x: ['background: skyblue' if i else '' for i in sub_df['MSBS']!=''])
    
    return df_style
#-------------------------------------------------------------------------#

st.title('PHÒNG KHÁM NHÀ MÌNH')

path1 = st.secrets['path1']
path2 = st.secrets['path2']

main_df, short_df, text_df, bn_cols, bs_cols, total_close, total_open, dsbs, date = Load_Data(path1, path2)

col1, col2 = st.beta_columns(2)
with col1:
    st.header('Giao diện cho bác sĩ')
with col2:
    msbs = st.text_input('Vui lòng nhập mã số bác sĩ:').upper().strip()

if (msbs not in dsbs) & (msbs!=msbs_admin):
    st.stop()
else:
    st.sidebar.table(pd.DataFrame(demo.keys(), index=demo.values(), columns= ['QUY ƯỚC']).style.apply(lambda x: [color[i] for i in x]))
    st.sidebar.markdown('**Chọn mã bệnh nhân để xem chi tiết triệu chứng khác, nhu cầu cần hỗ trợ và đánh giá theo dõi của bác sĩ.**')
    st.sidebar.markdown('*Thông báo lỗi hoặc góp ý đến email:*')
    st.sidebar.markdown('<a href="mailto:lamxuantam@gmail.com">lamxuantam@gmail.com</a>', unsafe_allow_html=True)
    
    bn_open, bn_close = Filter_Patients(msbs)

# OPEN CASE
    with col1:
        timerange = st.slider('Thể hiện cập nhật trong thời gian: :', min_value= date[0], max_value=date[1], value=(date[1]-timedelta(3), date[1]))
        st.info('Bác sĩ hiện phụ trách ' + str(len(bn_open)) + ' bệnh nhân.')

    if len(bn_open)>0:
        with col2:    
            msbn_open = st.selectbox('Chọn bệnh nhân', ['Overview']+ pd.Series(bn_open).sort_values().tolist())
            
        if msbn_open =='Overview':
            with col2:
                rating = st.checkbox('Sắp xếp theo thứ tự ưu tiên')
            st.subheader('Cập nhật gần nhất của tất cả bệnh nhân')
            if rating:
                st.table(Show_DF(short_df[short_df['MSBN'].isin(bn_open)].sort_values(['rate'], ascending=False),timerange, bn_cols, [], [*bs_cols,*['rate']], waiting=True))
            else:
                st.table(Show_DF(short_df[short_df['MSBN'].isin(bn_open)],timerange, bn_cols, [], [*bs_cols,*['rate']], waiting=True))
            
        else:
            with col2:
                combine = st.checkbox('Xem dồn các cột', value=True)
            st.subheader('Tất cả cập nhật của bệnh nhân ' + msbn_open)
            if combine:
                st.table(Show_TextDF(text_df[text_df['MSBN']==msbn_open], timerange))
            else:
                st.table(Show_DF(main_df[main_df['MSBN']==msbn_open],timerange, bn_cols, bs_cols))
    
    
    st.write('='*200)

# CLOSE CASE

    col3, col4 = st.beta_columns(2)
    with col3:
        st.warning('Bác sĩ đã phụ trách kết thúc ' + str(len(bn_close)) + ' bệnh nhân.')

    if len(bn_close)>0:
        with col3:
            extra = st.checkbox('Hiện các ca đã kết thúc')
        if extra:
            with col4: 
                msbn_close = st.selectbox('Chọn bệnh nhân', ['Overview']+pd.Series(bn_close).sort_values().tolist())

            if msbn_close =='Overview':
                st.subheader('Cập nhật sau cùng của tất cả bệnh nhân')
                st.table(Show_DF(short_df[short_df['MSBN'].isin(bn_close)],date, bn_cols, [], [*bs_cols,*['rate']], waiting=True))
            else:
                st.subheader('Tất cả cập nhật của bệnh nhân ' + msbn_close)
                st.table(Show_TextDF(text_df[text_df['MSBN']==msbn_close], date))
