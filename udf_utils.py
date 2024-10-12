import re
from datetime import datetime

class extract:

    def __init__(self, file_content):
        self.file_content = file_content.strip()
    
    def extract_file_name(self):
        data = self.file_content.split('\n')[0]
        return data

    def extract_position(self):
        data = self.file_content.split('\n')[0]
        return data

    def extract_class_code(self):
        try:
            match = re.search(r'(Class Code:)\s+(\d+)',self.file_content)
            if match:
                return match.group(2)
            else:
                return None
        except Exception as e:
            raise ValueError(f'Error extracting class code: {e}')
        
    def extract_start_date(self):
        try:
            match = re.search(r'(Open [Dd]ate:)\s+(\d\d-\d\d-\d\d)',self.file_content)
            if match:
                date = datetime.strptime(match.group(2),'%m-%d-%y') # datestring to datetime obj
                return date 
            else:
                return None
        except Exception as e:
            raise ValueError(f'Error extracting start_date: {e}')

    def extract_end_date(self):
        try:
            matches = re.findall(r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s(\d{1,2},\s\d{4})',self.file_content,re.IGNORECASE)
            if matches:
                tuple_data = matches[3]
                month = tuple_data[0]
                day_year = tuple_data[1]

                month_number = datetime.strptime(month,'%B').month
                day = int(day_year.split(',')[0])
                year = int(day_year.split(',')[-1].strip())

                date_obj = datetime(year,month_number,day)
                return date_obj
            else:
                return None
        except Exception as e:
            raise ValueError(f'Error extracting end_date: {e}')


    def extract_salary(self):
        try:
            matches = re.findall(r'\$(\S+)',self.file_content)
            
            if matches:
                data = [int(match.replace(',','')) for match in matches]
                start_salary = min(data)
                end_salary = max(data)
                return {'start_salary': start_salary,'end_salary': end_salary}
            else:
                return {'start_salary': None,'end_salary': None}
        except Exception as e:
            raise ValueError(f'Error extracting salary: {e}')

    def extract_requirements(self):
        try:
            match = re.search(r'(REQUIREMENTS?/\s?MINIMUM QUALIFICATIONS?)(.*)(PROCESS NOTES?)',self.file_content,re.DOTALL)
            return match.group(2).strip() if match else None
        except Exception as e:
            raise ValueError(f'Error extracting requirements: {e}')
        

    def extract_notes(self):
        try:
            match = re.search(r'NOTES?:\s*\n+(.*?)(?=\n{2,}DUTIES)', self.file_content, re.DOTALL)
            return match.group(1).strip() if match else None
        except Exception as e:
            raise ValueError(f'Error extracting notes: {e}')
        

    def extract_duties(self):
        try:   
            match = re.search(r'DUTIES\n\n(.*)\n\nREQUIREMENTS?', self.file_content, re.DOTALL)
            
            if match:
                # print(match.group(1))
                return match.group(1).strip()
            else:
                return None
        except Exception as e:
            raise ValueError(f'Error extracting Duties: {e}')

    def extract_selection(self):
        try:
            match = re.findall(r'([A-Z][a-z]+)(\s\.\s)+',self.file_content)
            print(match)
            data = [ele[0] for ele in match] if match else None
            return data
        except Exception as e:
            raise ValueError(f'Error extracting selection {e}')

    def extract_experience_length(self):
        try:
            match = re.search(r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)\s?-?years?',self.file_content)
            return match.group(1).strip() if match else None
        except Exception as e:
            raise ValueError(f'Error extracting experience length')

    def extract_apply_location_udf(self):
        try:
            match = re.search(r'WHERE\sTO\sAPPLY\n\n(.*)\n\n[A-Z]+',self.file_content)
            return match.group(1).strip() if match else None
        except Exception as e:
            raise ValueError(f'Error extracting job location {e}')

