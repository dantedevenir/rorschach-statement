import pandas as pd
import time
from os import getenv
from nite_howl import NiteHowl, minute

class Receiver:
    def __init__(self):
        self.broker = getenv('BROKER')
        self.topic = getenv('TOPIC')
        self.group = getenv('GROUP')
        self.env =  getenv('ENV_PATH')
        self.howler = NiteHowl(self.broker, self.group, str(self.topic).split(","), "statement")
        
    def catch(self):
        radar_generator = self.howler.radar()
        primary = None
        secondary = None
        while True:
            try:
                minute.register("info", f"Searching topics for analize statement...")
                table, topic, key, headers, type = next(radar_generator)
                if topic == "statement":
                    if headers and "side" in headers.keys() and headers["side"] == "right":
                        secondary = table
                    elif headers and "side" in headers.keys() and headers["side"] == "left":
                        primary = table
                
                    if primary and secondary:
                        self.commit(primary, key, secondary)
                        primary = None
                        secondary = None
                else:
                    minute.register("info", f"That larva '{key}' won't need me.")
            except StopIteration:
                radar_generator = self.howler.radar()
            time.sleep(0.1)
        
    def commit(self, csv: pd.DataFrame, key, crm = pd.DataFrame([])):            
        for index, _ in csv.iterrows():
            left_row = csv.iloc[index]
            right_row = crm.iloc[index] if not crm.empty else {"ffm_subscriber_id": "", "salesorder_no": 0, "member_id": ""}
                       
            self.howler.send(
                'warehouse',
                msg = { 
                    "id": [
                        left_row["ffm_subscriber_id"],
                        right_row['salesorder_no'],
                        left_row["member_id"],
                        left_row["ffm_app_id"],
                    ]
                },
                key = 'request',
                headers = { 
                    "module": "entity"
                }
            )
            
            #send key to warehouse for validate statement
            #for k, v in existing_snapshots.items():
            #    if key == k[:4]:
            #        existing_statement_status = k[6]
            #        existing_statement = session.query(Statement).filter_by(id=k[5]).scalar()
            #        break
            
            
'''             
            comparison_dict = {key: False for key in csv.keys()}
            if existing_statement:
                policy_df = self.refactor_statement(existing_statement, csv.keys())
                comparison_policy = policy_df == pd.Series(comparison_dict)
                if all(comparison_policy):
                    changes = False
            
            if not crm.empty:
                columns_to_ignore_empty_strings = ['member_id','ffm_subscriber_id']
                columns_to_ignore_empty_integers = ['salesorder_no']
                diff = (left_row == right_row)
                
                diff[columns_to_ignore_empty_strings] = diff[columns_to_ignore_empty_strings] & ~(left_row[columns_to_ignore_empty_strings].astype(str) == '')
                diff[columns_to_ignore_empty_integers] = diff[columns_to_ignore_empty_integers] & ~(left_row[columns_to_ignore_empty_integers].astype(int) == 0)
                
                comparison_dict = diff

                if existing_statement:
                    comparison_policy = policy_df == comparison_dict
                    if all(comparison_policy):
                        changes = False

                statement = existing_statement
            else:
                changes = True

            ##############################################################################################################
            """
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
diff_table = pd.concat(
[
    left_row,
    right_row,
    diff
],
axis=1,
keys=["LEFT", "RIGHT", "DIFF"],
)
print(diff_table)
            """
            ##############################################################################################################

            if existing_statement:
                entity = session.query(Entity).filter_by(id=existing_statement.entity_id).scalar()
                if changes:
                    statement = self.create_statement(comparison_dict, session)
                    entity.statements.append(statement)
            else:
                entity = Entity(
                    ffm_subscriber_id = key[0],
                    salesorder_no = key[1],
                    member_id = key[2],
                    ffm_app_id = key[3]
                )
                statement = self.create_statement(comparison_dict, session)
                entity.statements.append(statement)

            session.add(entity)
            session.commit()

            snapshot = SnapShot(
                entity_id = entity.id,
                provider_id = provider.id,
                timestamp = datetime.now(timezone.utc),
                status = existing_statement_status if existing_statement_status and not changes else None,
            )
            
            entity.snapshots.append(snapshot)
            session.commit() '''