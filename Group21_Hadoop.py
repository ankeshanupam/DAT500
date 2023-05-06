
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class MRCountSum(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    def mapper(self, _, line):
        line = line.strip()
        text = line.split()
        Ven_ID = text[0]
        Pick_Date = text[1]
        Pick_Time = text[2]
        Drop_Date = text[3]
        Drop_Time = text[4]
        Pass_Count = text[5]
        Trip_Dist = text[6]
        PU_LocID = text[9]
        DO_LocID = text[10]
        Pay_Type = text[11]
        Fare = text[12]
        Extra = text[13]
        Tax = text[14]
        Tip = text[15]
        Toll = text[16]
        Imp_S = text[17]
        Tot = text[18]
        Cong_Sur = text[19]
        yield f"{Ven_ID} ,{ Pick_Date} ,{ Pick_Time},{ Drop_Date},{ Drop_Time},{ Pass_Count},{ Trip_Dist},{ PU_LocID},{ DO_LocID},{ Pay_Type},{ Fare},{ Extra},{ Tax},{ Tip},{ Toll},{Imp_S},{Tot},{Cong_Sur}", 1


    def reducer(self, key, values):
        yield None, key

if __name__ == '__main__':
    MRCountSum.run()

