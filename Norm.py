#Cade Stephens
#cs 5300
#Norm.py
#November 3rd

import itertools
from itertools import combinations
from collections import defaultdict
import pandas as pd
import re

#global vars for easy use
nonAtomic = []
attr= []
data=[]
mvd=[]
class DatabaseNormalizer:
    def __init__(self):
        self.relations = []
        self.functional_dependencies = []
        self.highest_normal_form = None
        self.non_atomic_attributes = []
        self.created_relations = set()
        
    # Parser to read the input Excel file and parse the database structure
    class AdvancedDatabaseParser:
        def __init__(self, file_path):
            global nonAtomic,attr
            self.sheet = pd.ExcelFile(file_path)
            self.data = pd.read_excel(file_path, sheet_name=self.sheet.sheet_names[0], header=None)
            
            # Initialize storage for parsed results
            self.num_elements = 0
            self.num_data_tuples = 0
            self.primary_keys = []
            self.candidate_keys = []
            self.functional_dependencies = defaultdict(list)
            self.mvd_list = defaultdict(list)
            self.non_atomic_list = defaultdict(list)
            self.data_tuples = []  
            self.relations = []
            self.functional_dependencies_list = []

            # Parse the database structure
            self.parse_database_structure()
            # Format parsed data to match the desired output structure
            self.format_parsed_data()
            attribute_row = 4  # always put your atributtes in row 4(5th actual row)
            attributes = self.data.iloc[attribute_row, :self.num_elements].tolist()
            attr=attributes
            self.relations = []

            self.relations.append({
                'name': "table",
                'attributes': attributes,
                'primary_keys': self.primary_keys,
                'is_new': False
            })

        def parse_database_structure(self):
            global data,nonAtomic,mvd


            # Step 1: Read number of elements and data tuples from Rows 1 and 2, Column A
            self.num_elements = int(self.data.iloc[0, 0])
            self.num_data_tuples = int(self.data.iloc[1, 0])
            
            # Step 2: Parse the data tuples
            data_start_row = 5  # Data starts from row index 5 
            data_end_row = data_start_row + self.num_data_tuples

            # Create a dictionary to store each data point with its corresponding attribute name and row index
            self.data_tuples = []
            attributes = self.data.iloc[4, :self.num_elements].tolist()
            for row_index in range(data_start_row, data_end_row):
                row_data = self.data.iloc[row_index, :self.num_elements].tolist()
                data_entry = {
                    'row_index': row_index,
                    'data': {attributes[col_index]: row_data[col_index] for col_index in range(self.num_elements)}
                }
                self.data_tuples.append(data_entry)
            #print("data tuples",self.data_tuples)
            data=self.data_tuples
            
            # Step 3: Determine where primary keys and candidate keys are located
            primary_key_row = 7 + self.num_data_tuples 
            candidate_key_row = primary_key_row + 1
            
            # Step 4: Parse Primary Key(s)
            primary_key_str = str(self.data.iloc[primary_key_row, 0]).replace("Primary Key:", "").strip()
            if primary_key_str.startswith("{") and primary_key_str.endswith("}"):
                # If it's a superkey with multiple elements
                self.primary_keys = [key.strip() for key in primary_key_str[1:-1].split(",")]
            else:
                # If it's a single primary key element
                self.primary_keys = [primary_key_str.strip()]
            
            # Step 5: Parse Candidate Keys
            candidate_key_str = str(self.data.iloc[candidate_key_row, 0]).replace("Candidate Keys:", "").strip()
            if candidate_key_str.lower() != "none":
                if candidate_key_str.startswith("{") and candidate_key_str.endswith("}"):
                    self.candidate_keys = [key.strip() for key in candidate_key_str[1:-1].split(",")]
                else:
                    self.candidate_keys = [candidate_key_str.strip()]
            
            # Step 6: Parse Functional Dependencies
            fd_start_row = candidate_key_row + 2
            fd_rows = self.data.iloc[fd_start_row:].dropna(how="all")
            
            for _, row in fd_rows.iterrows():
                fd_str = str(row[1]).strip()  # Functional dependencies are in Column B
                if "-->" not in fd_str and "-->>" not in fd_str:
                    # Skip rows that do not contain valid FD syntax
                    continue
                
                is_mvd = "-->>" in fd_str
                is_non_atomic = "(a non-atomic attribute)" in fd_str
                
                # Remove "(a non-atomic attribute)" if present
                fd_str = fd_str.replace("(a non-atomic attribute)", "").strip()
                if is_non_atomic:
                    nonAtomic.append(fd_str.split('-->')[1].strip())
                
                # Split FD into determinant and dependent parts(Left hand side and right hand side)
                if is_mvd:
                    lhs, rhs = fd_str.split("-->>")
                else:
                    lhs, rhs = fd_str.split("-->")
                    
                lhs = self.parse_fd_elements(lhs)
                rhs = self.parse_fd_elements(rhs)
                
                # Store in functional dependencies
                if is_mvd:
                    self.mvd_list[tuple(lhs)].extend(rhs)
                else:
                    self.functional_dependencies[tuple(lhs)].extend(rhs)
                if is_non_atomic:
                    self.non_atomic_list[tuple(lhs)].extend(rhs)
            
            #print(nonAtomic)
            mvd=self.mvd_list
            # print("")
            # print("mvd list:",self.mvd_list)
            # print("")

        def parse_fd_elements(self, fd_part):
            fd_part = fd_part.strip()
            if fd_part.startswith("{") and fd_part.endswith("}"):
                return [elem.strip() for elem in fd_part[1:-1].split(",")]
            return [fd_part]

        def format_parsed_data(self):
            self.relations = [
                {
                    'name': 'DatabaseRelation',  
                    'attributes': list(self.data.columns[:self.num_elements]),
                    'primary_keys': self.primary_keys,
                    'is_new': False
                }
            ]

            # Format functional dependencies to match the desired format
            self.functional_dependencies_list = [
                {'lhs': list(lhs), 'rhs': list(set(rhs))}  
                for lhs, rhs in self.functional_dependencies.items()
            ]









    def parse_input(self):
        # hardcodeed test input or not
        input_type = "2"
        #previlusly hardcoded input was here for testing
        if input_type == '1':
            # Hardcoded input for CoffeeShopData 
            self.relations = []
        elif input_type == '2':

            file_path = "D:/programing/2024/database/turnin/input.xlsx"                                       # Adjusted to match your directory, i hardcoded the path for testing, i included .xlsx files for testing in repo.
            parser = self.AdvancedDatabaseParser(file_path)

            # Extract relations and functional dependencies from parser
            self.relations = parser.relations
            self.functional_dependencies = parser.functional_dependencies_list
            #print(self.functional_dependencies)


        # highest normal form 4 being BCNF and 5 and 6 bein 4nf and 5NF
        while True:
            try:
                self.highest_normal_form = int(input("Enter the target highest normal form (1, 2, 3, 4, 5, 6): "))
                if 1 <= self.highest_normal_form <= 6:
                    break
                else:
                    print("Please enter a valid number between 1 and 6.")
            except ValueError:
                print("Invalid input. Please enter a number between 1 and 6.")



    #for cleaning the data of any duplicates or useless relations
    def remove_duplicates(self):
        unique_relations = []
        seen_signatures = set()

        for relation in self.relations:
            if len(relation['attributes']) <= 1:
                continue

            signature = frozenset(relation['attributes']), frozenset(relation['primary_keys'])

            if signature not in seen_signatures:
                seen_signatures.add(signature)
                unique_relations.append(relation)

        self.relations = unique_relations


    def normalize_to_1nf(self, relation):
        global nonAtomic
        if not relation.get('is_new', False):
            #print(f"Attributes for relation '{relation['name']}': {relation['attributes']}")
            self.non_atomic_attributes = nonAtomic
        else:
            self.non_atomic_attributes = []               # problem right here
        
        if self.non_atomic_attributes:
            for attr in self.non_atomic_attributes:
                determinant = next((fd['lhs'] for fd in self.functional_dependencies if attr in fd['rhs']), None)
                new_relation_name = f"{relation['name'][:3]}_{attr[:3]}_A"
                new_relation = {
                    'name': new_relation_name,
                    'attributes': determinant + [attr],
                    'primary_keys': determinant + [attr], 
                    'is_new': True
                }
                self.relations.append(new_relation)
                relation['attributes'].remove(attr)

        return relation

    def normalize_to_2nf(self, relation):
        global nonAtomic
        # Dict to track created relations
        created_relations = {}

        keys = set(relation['primary_keys'])
        partial_dependencies = []

        # Identify partial dependencies
        for fd in self.functional_dependencies:
            lhs_set = set(fd['lhs'])
            if lhs_set.issubset(keys) and not lhs_set == keys:
                partial_dependencies.append(fd)

        # Process each partial dependency to create a new relation
        for fd in partial_dependencies:
            lhs = fd['lhs']
            rhs = [attr for attr in fd['rhs'] if attr not in nonAtomic]

            if rhs:
                new_relation = {
                    'attributes': lhs + rhs,
                    'primary_keys': lhs,
                    'is_new': True
                }

                signature = frozenset(new_relation['attributes']), frozenset(new_relation['primary_keys'])

                if signature not in created_relations:
                    new_relation_name = f"{relation['name'][:3]}_{'_'.join([attr[:3] for attr in rhs])}_2NF"
                    new_relation['name'] = new_relation_name

                    # Add the new relation 
                    created_relations[signature] = new_relation
                    self.relations.append(new_relation)
                    relation['attributes'] = [attr for attr in relation['attributes'] if attr not in rhs]

        return relation


    #function to normalize to 3NF by finding the transitive dependencies
    def normalize_to_3nf(self, relation):
        new_relations = []
        attributes_to_remove = set()
        for fd in self.functional_dependencies:
            lhs = fd['lhs']
            rhs = fd['rhs']

         
            if any(attr in relation['attributes'] and attr not in relation['primary_keys'] for attr in lhs):

                new_relation_name = f"{relation['name'][:3]}_{'_'.join([attr[:3] for attr in rhs])}_3NF"
                new_relation = {
                    'name': new_relation_name,
                    'attributes': lhs + rhs,
                    'primary_keys': lhs,
                    'is_new': True
                }


                if not any(r['attributes'] == new_relation['attributes'] and r['primary_keys'] == new_relation['primary_keys'] for r in self.relations):
                    new_relations.append(new_relation)

                attributes_to_remove.update(rhs)
        relation['attributes'] = [attr for attr in relation['attributes'] if attr not in attributes_to_remove]
        self.relations.extend(new_relations)

        return relation



    def normalize_to_bcnf(self, relation):
        for fd in self.functional_dependencies:
            if set(fd['lhs']) not in [set(r['primary_keys']) for r in self.relations]:
                new_relation_name = f"{relation['name'][:3]}_{'_'.join([attr[:3] for attr in fd['rhs']])}_BCNF"
                new_relation = {
                    'name': new_relation_name,
                    'attributes': fd['lhs'] + fd['rhs'],
                    'primary_keys': fd['lhs'] + fd['rhs'],   
                    'is_new': True
                }
                self.relations.append(new_relation)
                relation['attributes'] = [attr for attr in relation['attributes'] if attr not in fd['rhs']]
        return relation

    def normalize_to_4nf(self, mvd_data):
        """
        Normalize relations to 4NF by identifying and decomposing Multi-Valued Dependencies (MVDs).
        :param mvd_data: A dictionary containing MVDs, e.g., {('OrderID',): ['DrinkID', 'FoodID']}
        """
        new_relations = []

        # Iterate over each MVD in the given mvd_data
        for determinant_tuple, dependent_attributes in mvd_data.items():
            determinant = list(determinant_tuple)

            # Find the table containing the MVD attributes
            for relation in self.relations:
                if all(attr in relation['attributes'] for attr in determinant + dependent_attributes):
                    for dependent in dependent_attributes:
                        remaining_attributes = [attr for attr in relation['attributes'] if attr not in dependent_attributes]

                        new_relation = {
                            'name': f"{relation['name']}_{dependent}_MVD1",
                            'attributes': determinant + [dependent],
                            'primary_keys': determinant + [dependent],
                            'is_new': True
                        }

                        # Append the new relation to the list
                        new_relations.append(new_relation)

                    # Update the original relation to remove the dependent attributes
                    relation['attributes'] = remaining_attributes
                    relation['primary_keys'] = [key for key in relation['primary_keys'] if key not in dependent_attributes]

                    # Break to ensure we only decompose the relevant table once
                    break

        self.relations.extend(new_relations)

        # Remove duplicates, often created by MVD decomposition the way it's implemented
        self.remove_duplicates()






    #couldnt achieve 5nf normalization
    def normalize_to_5nf(self, relation):
        

        return relation
    

    def rename_relations(self):
        # Print current state of all relations and allow user to rename them
        print("\nFinal Normalized Schema:")
        for relation in self.relations:
            print(f"\nTable: {relation['name']}")
            print(f"Attributes: {', '.join(relation['attributes'])}")
            print(f"Primary Keys: {', '.join(relation['primary_keys'])}")

            new_name = input("Enter a new name for this table, or press Enter to skip: ").strip()
            if new_name:
                relation['name'] = new_name
                print(f"Table renamed to: {new_name}")

    def print_formatted_tables(self):
        for relation in self.relations:
            print(f"\nTable: {relation['name']}")
            
            separator = '+'.join(['-' * (len(attr) + 2) for attr in relation['attributes']])
            separator = f"+{separator}+"

            print(separator)

            attributes_row = '|'.join([f" {attr} " for attr in relation['attributes']])
            attributes_row = f"|{attributes_row}|"
            print(attributes_row)
            
            print(separator)

            primary_keys = ', '.join(relation['primary_keys'])
            print(f"Primary Keys: {primary_keys}\n")




    # Normalize the database to the target highest normal form through a steping process
    def normalize(self):
        global mvd
        new_relations = []
        for relation in self.relations:
            relation = self.normalize_to_1nf(relation)
            new_relations.append(relation)
        self.relations = new_relations

        if self.highest_normal_form >= 2:
            new_relations = []
            for relation in self.relations:
                relation = self.normalize_to_2nf(relation)
                new_relations.append(relation)
            self.relations = new_relations

        if self.highest_normal_form >= 3:
            new_relations = []
            for relation in self.relations:
                relation = self.normalize_to_3nf(relation)
                new_relations.append(relation)
            self.relations = new_relations

        if self.highest_normal_form >= 4:
            new_relations = []
            for relation in self.relations:
                relation = self.normalize_to_bcnf(relation)
                new_relations.append(relation)
            self.relations = new_relations

        if self.highest_normal_form >= 5:
            self.normalize_to_4nf(mvd)

        if self.highest_normal_form >= 6:
            new_relations = []
            for relation in self.relations:
                relation = self.normalize_to_5nf(relation)
                new_relations.append(relation)
            self.relations = new_relations
        self.remove_duplicates()
        #remove any duplicates or useless relations   
        
         
    # Print the normalized schema in a formatted manner
    def generate_normalized_schema(self):
        print("\nNormalized Schema:")
        for relation in self.relations:
            print(f"Table: {relation['name']}")
            print(f"Attributes: {', '.join(relation['attributes'])}")
            print(f"Primary Key: {', '.join(relation['primary_keys'])}")
            print()

 

#main function to run the program
if __name__ == "__main__":
    normalizer = DatabaseNormalizer()
    normalizer.parse_input()
    normalizer.normalize()
    normalizer.rename_relations()
    normalizer.print_formatted_tables()

















