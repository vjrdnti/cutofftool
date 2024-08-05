from flask import Flask, request, jsonify, render_template, redirect, url_for
import time
import fitz
import pdfplumber
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import lxml
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

filepath2 = ''

def split_code_and_name(text):
    parts = text.split(" - ", 1)

    if len(parts) == 2:
        text1 = parts[0]
        text2 = parts[1]
    else:
        #exception
        text1 = text
        text2 = ""
    return text1,text2
    
def extract_text_details(pdf_path):
    document = fitz.open(pdf_path)
    text_details = []

    college_name = ""
    college_code = ""
    branch_name = ""
    branch_code = ""
    current_level = ""

    tn = 0
    for page_num in range(len(document)):
        page = document.load_page(page_num)
        for text in page.get_text("dict")["blocks"]:
            if "lines" in text:
                for line in text["lines"]:
                    for span in line["spans"]:
                        if ' - ' in span["text"] and span["color"]==0 and 'Un-Aided' not in span["text"]:
                            college_code, college_name = split_code_and_name(span["text"])
                        elif span["color"] == 8388608:
                            branch_code, branch_name = split_code_and_name(span["text"])
                        elif span["color"] == 255:
                            current_level = span["text"]
                            tn+=1
                            row = {
                                "College Code": college_code,
                                "College Name": college_name,
                                "Branch Code": branch_code,
                                "Branch Name": branch_name,
                                "Level": current_level,
                                "table no": tn
                            }
                            text_details.append(row)


    document.close()
    print("text done")
    return text_details

def extract_tables(pdf_path):
    tables_details = []
    i=1
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            if '-' not in page.extract_text():
                continue
            tables = page.extract_tables()
            for table in tables:
                tables_details.append({
                    "page": page_num + 1,
                    "table": table,
                    "table no": i
                })
                i+=1
    return tables_details
    
def combine_text_table(text_d, table_d):
    data = []
    df = pd.DataFrame()
    t_counter = 0
    reshaped_data = {'Category': [], 'Stage': [], 'Value': [], 'table no': []}
    for t in table_d:
        data = t["table"]
        headers = data[0][1:]
        rows = data[1:]
        for row in rows:
            stage = row[0]
            for i, value in enumerate(row[1:]):
                reshaped_data['Category'].append(headers[i])
                reshaped_data['Stage'].append(stage)
                reshaped_data['Value'].append(value)
                reshaped_data['table no'].append(t["table no"])

        # Create the DataFrame
    df = pd.DataFrame(reshaped_data)
    df2 = pd.DataFrame(text_d)

    combined_df = pd.merge(df2, df, on='table no', how='right')
    combined_df[['cutoff', 'percentile']] = combined_df['Value'].str.split('\n', expand=True)

    combined_df['cutoff'] = pd.to_numeric(combined_df['cutoff'])
    combined_df['percentile'] = combined_df['percentile'].str.replace('[()]', '', regex=True).astype(float)

    combined_df.drop('Value', axis=1, inplace=True)

    return combined_df


UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
ALLOWED_EXTENSIONS = {'pdf'}

CSV_SAVE_FOLDER = './processed_csv/'
os.makedirs(CSV_SAVE_FOLDER, exist_ok=True)

course_functions = {
    "MBA/MMS": ("mba_cutoffs", "create_mba_dataframe"),
    "B .Pharmacy & Post Graduate Pharm. D": ("pharmacy_cutoffs", "create_pharmacy_dataframe"),
    "Course Name : Pharmacy": ("dsp1", "create_dsp_dataframe"),
    "MCA": ("mca_cutoffs", "create_mca_dataframe"),
    "Direct second Year": ("dse_cutoffs", "create_dse_dataframe"),
    "Architecture (M. ARCH)": ("architecture_cutoffs", "create_arch_dataframe"),
    "Full Time Degree Course in Architecture": ("b_architecture_cutoffs", "create_barch_dataframe"),
    # Add other courses here
}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    

def read_pdf_header(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page = pdf.pages[0]
            text = first_page.extract_text()
            print(text[:500])
            return text[:500] if text else 'No text found in the header'
    except Exception as e:
        print(f"Error reading PDF: {str(e)}")
        return f"Error reading PDF: {str(e)}"

def find_keywords(text):
	keywords = [
		'Technology (Integrated 5 Years)',
		"MBA/MMS", "MCA", "Post Graduate Degree Course in Engineering and Technology",
		"Degree Courses In Engineering and Technology & Master of Engineering and Technology", "Course Name : Pharmacy",
		"B .Pharmacy & Post Graduate Pharm. D", "Direct second Year", "Architecture (M. ARCH)",
		"Full Time Degree Course in Architecture",
		"Degree Courses In Engineering and Technology & Master of Engineering and Technology",
		"First Year Under Graduate Technical Courses in Engineering and Technology",
		"FirstYear Under Graduate Technical Courses in Engineering and Technology"
	# Add other keywords as needed
	]
	found = []
	for i in keywords:
		if i in text:
			found.append(i)
			break
	return found

def decode_cf_email(encoded_email):
    r = int(encoded_email[:2], 16)
    email = ''.join([chr(int(encoded_email[i:i+2], 16) ^ r) for i in range(2, len(encoded_email), 2)])
    return email

def long_running_task(file_path, filepath2):
	header = read_pdf_header(file_path)
	with open('./progress.txt', 'w') as f:
		if header != 'No text found in the header' or header[0:17]=='Error reading PDF':
			keyl = find_keywords(header)
			if len(keyl)>0:
				if keyl[0]=="Technology (Integrated 5 Years)":
					pdf_path = file_path	
					text_details = extract_text_details(pdf_path)
					f.write(f"pdf text extracted\n")
					f.flush()
					tables_details = extract_tables(pdf_path)
					f.write(f"pdf table linked\n")
					f.flush()
					dfx =combine_text_table(text_details, tables_details)

					dfx = dfx.fillna('N/A')

					code_list = dfx["College Code"].unique().tolist()
					merged_df_list = []
					for code in code_list:
						url = f'https://cetcell.mahacet.org/search-institute-deatils/?getinstitutecode={code}'
						response = requests.get(url)
						if response.status_code == 200:
							page_content = response.text
							soup = BeautifulSoup(page_content, 'html.parser')
							lt = soup.find_all('table')
							encoded_emails = soup.find_all('a', {'class': '__cf_email__'})
							decoded_emails = []

							for encoded_email in encoded_emails:
								data_cfemail = encoded_email['data-cfemail']
								decoded_email = decode_cf_email(data_cfemail)
								decoded_emails.append(decoded_email)

							ci =  str(lt[0])
							#print(ci)
							try:
								ci = pd.read_html(ci)
							except:
								print(f'{ci}\n*******')
								result = pd.DataFrame([{'Department Name':np.nan, 'Institute Name':np.nan, 'District':np.nan, 'City':np.nan,'University':np.nan,
								'Institute Status':np.nan, 'Minority Status':np.nan, 'E-Mail ID':np.nan, 'College Code':code,
								'Address':np.nan, 'Taluka':np.nan, 'PIN Code':np.nan, 'Establishment Year':np.nan,
								'Autonomy Status':np.nan, 'Phone Number':np.nan, 'Website URL':np.nan, 'Course Name':np.nan,
								'Course Type':np.nan, 'Branch Name':np.nan, 'Sanction Intake':np.nan}])
								#merged_df = df.merge(result, on=['College Code'], how='left')
								merged_df_list.append(result)
								print(f'{code} done unsuccesfully')
								f.write(f"college code {code} done unsuccesfully\n")
								f.flush()
								continue
							data = []
							i=0
							row={}
							while i<4:
								for j in range(len(ci[0][i].tolist())):
									row[f"{ci[0][i].tolist()[j]}"] = ci[0][i+1].tolist()[j]
								i+=2
							data.append(row)
							df1 = pd.DataFrame(data)
							df1["E-Mail ID"] = decoded_emails
							if len(df1.columns)!=16:
							  print(f'for {code} num of cols is {len(df1.columns)}')
							df2 =  pd.read_html(str(lt[1]))
							df2 = df2[0]
							result = pd.concat([df2]*len(df1)).reset_index(drop=True)
							for col in df1.columns:
								result[col] = df1.iloc[0][col]
							result = result[df1.columns.tolist() + df2.columns.tolist()]
							result = result.rename(columns={"Sub Course Name":"Branch Name", "Institute code":"College Code"})
							result["College Code"] = result["College Code"].astype(float)
							#merged_df = df.merge(result, on=['College Code', 'Branch Name'], how='left')
							merged_df_list.append(result)
							print(f'{code} done')
						else:
							print(f'Failed to retrieve the page for {code}. Status code: {response.status_code}')
							f.write(f"Failed to retrieve the page for college code {code}. Status code: {response.status_code}\n")
							f.flush()
					f.write(f"college information scraped from portal\n")
					f.flush()
					final_df = pd.concat(merged_df_list, ignore_index=True)
					final_df = final_df.replace('', pd.NA)
					final_df = final_df.drop_duplicates()
					dfx['College Code'] = dfx['College Code'].astype(float) 
					final_df['College Code'] = final_df['College Code'].astype(float)
					merged_df = pd.merge(dfx, final_df, on=['College Code', 'Branch Name'], how='left')
					merged_df.to_csv(f'{filepath2}/final_output.csv')
					df = pd.read_csv(f'{filepath2}/final_output.csv')
					f.write('file saved to destination')
					f.flush()
					os.remove('./progress.txt')
					return url_for('indextwo') 			
			else:
				return 'no keywords'
				print('no keywords')	
		else:
			return 'no header'
			print('header')

def debug_driver(file_p):
    long_running_task(file_p)
    return f'File processed successfully'

#df = pd.read_csv('{filepath2}/final_output.csv')
#df = df.dropna(subset=['cutoff','percentile','Category'])
# Extract unique categories

#print(df.District)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
global progress

@app.route('/indextwo', methods=['GET', 'POST'])
def indextwo():
	#print(df.Category)
	return render_template("index.html")


@app.route('/categoryi', methods=['GET'])
def get_categoriesi():
	#if request.method=='GET':
	df = pd.read_csv(f'{filepath2}/final_output.csv')
	return jsonify(df['Category'].replace('', pd.NA).dropna().unique().tolist())

@app.route('/category', methods=['GET'])
def get_categories():
	#if request.method=='GET':
	percentile = float(request.args.get('percentile'))
	df = pd.read_csv(f'{filepath2}/final_output.csv')
	df = df.dropna(subset=['cutoff','percentile','Category'])
	filtered_df = df.loc[df['percentile']<=percentile]
	if request.args.get('category'):
		cat=request.args.get('category')
		filtered_df = df.loc[df['Category']==cat]
	return jsonify(filtered_df['Category'].replace('', pd.NA).dropna().unique().tolist())

@app.route('/degree', methods=['GET'])
def get_degrees():
	df = pd.read_csv(f'{filepath2}/final_output.csv')
	df = df.dropna(subset=['cutoff','percentile','Category'])
	if request.args.get('percentile'):
		percentile = request.args.get('percentile')
		filtered_df = df.loc[df['percentile']<=percentile]
		if request.args.get('category'):
			selected_category = request.args.get('category')
			degrees = filtered_df.loc[filtered_df['Category'] == selected_category]['Course Name'].replace('', pd.NA).dropna().unique().tolist()
			return jsonify(degrees)
		return jsonify(filtered_df['Course Name'].replace('', pd.NA).dropna().unique().tolist())
	return jsonify(df['Course Name'].replace('', pd.NA).dropna().unique().tolist())

@app.route('/branches', methods=['GET'])
def get_branches():
	df = pd.read_csv(f'{filepath2}/final_output.csv')
	df = df.dropna(subset=['cutoff','percentile','Category'])
	selected_degree = request.args.get('degree')
	if request.args.get('percentile') != '':
		percentile = float(request.args.get('percentile'))
	else:
		percentile = 0
	category = request.args.get('category')
	filtered_df = df[df['percentile']<=percentile]
	if request.args.get('category'):
		category = request.args.get('category')
		filtered_df = filtered_df[filtered_df['Category']==category]
	if selected_degree:
	    branches = filtered_df[filtered_df['Course Name'] == selected_degree]['Branch Name'].replace('', pd.NA).dropna().unique().tolist()
	    return jsonify(branches)
	return jsonify([])

@app.route('/districts', methods=['GET'])
def get_districts():
	df = pd.read_csv(f'{filepath2}/final_output.csv')
	df = df.dropna(subset=['cutoff','percentile','Category'])
	selected_branches = request.args.getlist('branches[]')
	percentile = float(request.args.get('percentile'))
	degree = request.args.get('degree')

	filtered_df = df[df['Course Name']==degree]
	filtered_df = filtered_df[filtered_df['percentile']<=percentile]

	#if request.args.getlist('category[]'):
	temp = request.args.get('category')
	filtered_df = filtered_df.loc[filtered_df['Category'] == temp]
	if selected_branches:
		districts = filtered_df.loc[filtered_df['Branch Name'].isin(selected_branches)]['District'].replace('', pd.NA).dropna().unique().tolist()
		if len(districts)==0:
			districts.append('')
	else:
		districts = filtered_df['District'].replace('', pd.NA).dropna().unique().tolist()
		if len(districts)==0:
			districts.append('')
	return jsonify(districts)
	#return jsonify([])
	

@app.route('/result', methods=['GET','POST'])
def result(): 
	percentile = float(request.args.get('percentile'))
	category = request.args.get('category')
	degree = request.args.get('degree')
	selected_branches = request.args.getlist('branches')
	selected_districts = request.args.getlist('districts')

	data = {
		"percentile": percentile,
		"category": category,
		"branches": selected_branches,
		"districts": selected_districts,
		"degree": degree
	}
	df = pd.read_csv(f'{filepath2}/final_output.csv')
	df = df.dropna(subset=['cutoff','percentile','Category'])
	filtered_df = df[(df['percentile'] <= percentile)]
	#print(f'hh: {selected_branches}') 
	#print(f'hh: {selected_districts}') 
	#print(filtered_df)
	if category:
		filtered_df = filtered_df.loc[filtered_df['Category'] == category]

	if degree:
		filtered_df = filtered_df.loc[filtered_df['Course Name'] == degree]

	if len(selected_branches)!=0:
		filtered_df = filtered_df.loc[filtered_df['Branch Name'].isin(selected_branches)]

	if len(selected_districts)!=0:
		print(selected_districts)
		filtered_df = filtered_df.loc[filtered_df['District'].isin(selected_districts)]
	
	# Sort the DataFrame by percentile in descending order
	filtered_df = filtered_df.sort_values(by='percentile', ascending=False)

	# Save the filtered data to a session or a temporary variable (simplified here)
	# In practice, you'd likely use a database or session storage for this
	filtered_data = filtered_df.to_dict(orient='records')
	#print(filtered_data)
	# Render the results page
	#return redirect(url_for('result') + '?' + query_string)
	#print(render_template('resultmain.html', data=filtered_data))
	return render_template('resultmain.html', data=filtered_data)


@app.route('/')
def index():
    return render_template('index0.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'No file part', 400
    file = request.files['file']
    if file.filename == '':
        return 'No selected file', 400

    filepath2 = request.form.get('destination')
    file_path = f"./{file.filename}"
    
    file.save(file_path)
    return redirect(long_running_task(file_path, filepath2))
    #df = df.dropna(subset=['cutoff','percentile','Category'])
    


@app.route('/progress')
def progress():
    try:
        with open('progress.txt', 'r') as f:
            lines = f.readlines()
            last_three_lines = lines[-3:]  # Get the last three lines
            content = ''.join(last_three_lines)
        return jsonify({'content': content})
    except FileNotFoundError:
        return jsonify({'content': ''}), 404

if __name__ == '__main__':
	#a = int(input('0 or 1 for mode: '))
	#if a==0:
	app.run(debug=True)
		#print('this and that')
	#elif a==1:
	#	print(debug_driver('./EN CAP Round III 2023-24.pdf'))

