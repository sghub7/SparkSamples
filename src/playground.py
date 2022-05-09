### custom validations
custom = [{"colname1": {"value_exists":{"values":["test"],"dist":[.10]},"null_count_threshold":0}}]

# custom = [{"colname1": {"value_exists":["test"]}}
#           ]

class CustomValidations:

    def value_exists(self,col,valueParams):
        dist=False
        print("value_exists --",valueParams)
        print(" col ---",col)
        if "dist" in valueParams.keys():
            print ("dist exists")
            dist =True
        for k,v in valueParams.items():
            print("******")
            print(k,v)
            if  dist:
                print("**** values")
                print(valueParams["values"])
                checkValues =valueParams["values"]


    def null_count_threshold(self,col,valueParams):
        print("null_count_threshold --",valueParams)



for i in custom:
    print("**Applying custom functions on below columns-- ")

    for col,validations in i.items():
        print("Column Name -",col)
        print("Validations -",validations)
        for validationFn,params in validations.items():
            fn=getattr(CustomValidations,validationFn)
            fn(None,col,params)

