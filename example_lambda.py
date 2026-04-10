# The following code is an example, do not keep it.
# It is just here to help you create your own code.
# You can try it by doing an invoke on this lambda, it works with the default invoke payload.

# TODO: First, import all the libraries you need:
import json
import math
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime
import copy
import pandas as pd



def parse_brackte_interval(key: str) -> Tuple[float, float, bool, bool]:
    """
    Parse une clé de type '[a; b]', '[a;]' ou '[; b]' en (low, high, low_inclusive, high_inclusive).
    - '[' et ']' impliquent bornes inclusives.
    - Bornes vides -> -inf ou +inf.
    - Espaces et séparateurs ' ; ' tolérés.
    """
    s = key.strip()
    # Vérifie les crochets
    if not (s.startswith('[') and s.endswith(']')):
        raise ValueError(f"Intervalle invalide (crochets attendus) : {key}")

    # Retire les crochets
    inner = s[1:-1].strip()

    # Sépare par ';'
    parts = [p.strip() for p in inner.split(';')]
    if len(parts) != 2:
        raise ValueError(f"Intervalle invalide (séparateur ';' manquant) : {key}")

    def to_num_or_inf_or_date(x: str, is_low: bool) -> float:
        if x == "" or x is None:
            return -math.inf if is_low else math.inf
        
        DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$") 
        if DATE_REGEX.match(x): 
            return ( datetime.strptime(x, "%Y-%m-%d").date())

        # Enlève espaces fines, milliers et remplace virgule par point
        x_clean = re.sub(r"[ \u00A0]", "", x).replace(",", ".")
        return float(x_clean)
    

    low_str, high_str = parts
    low = to_num_or_inf_or_date(low_str, is_low=True)
    high = to_num_or_inf_or_date(high_str, is_low=False)

    # Dans ta convention, les crochets sont toujours inclusifs.
    low_inclusive = True
    high_inclusive = True

    if low > high:
        raise ValueError(f"Borne basse > borne haute dans {key}")

    return low, high, low_inclusive, high_inclusive


def rate_from_bracket_dict(x: float, bracket_dict: Dict[str, float]) -> Optional[float]:
    """
    Retourne le taux pour la valeur x selon le dict d'intervalles au format '[a; b]': taux.
    S'il n'y a pas de correspondance, retourne None.
    """
    for k, rate in bracket_dict.items():
        low, high, low_inc, high_inc = parse_bracket_interval(k)

        cond_low  = (x > low)  or (low_inc  and x == low)
        cond_high = (x < high) or (high_inc and x == high)

        if cond_low and cond_high:
            return rate
    return None




def extract_offer_benefits_results(offer_data, effective_date, policy_period):
    # TODO: Check the calculation logic when summing up parameters expressed in percentage
    credibility_loadings = {}
    result = {}       
    total = {}
    plans = offer_data.get("plans", {})
    for plan_id, plan in plans.items():
        blocks = plan.get("blocks", {})
        for block_name, block in blocks.items():
            benefits = block.get("benefits", {})

            for benefit_name, benefit_data in benefits.items():

                if benefit_name not in result:
                    result[benefit_name] = {}
                    result[benefit_name]['technical_premium'] = benefit_data
 
    # Add the final price, premium rate and the credibility loading into the result by benefit level
    total_final_premium = 0
    for key in result.keys():
        benefit_name = key.lower()
        total_final_premium += result[key]["technical_premium"] 

    # Add total to result 
    total["technical_premium"] = total_final_premium 
    result["total"] = total

    return result



def extract_offer_blocks_results(offer_data, effective_date, policy_period):
    # TODO: Check the calculation logic when summing up parameters expressed in percentage
    credibility_loadings = {}
    result = {}       
    total = {}
    plans = offer_data.get("plans", {})
    for plan_id, plan in plans.items():
        blocks = plan.get("blocks", {})
        for block_name, block in blocks.items():
            benefits = block.get("benefits", {})
            result[block_name]= {}
            result[block_name]['technical_premium'] = 0 
            for benefit_name, benefit_data in benefits.items():
                result[block_name]['technical_premium'] = result[block_name]['technical_premium'] + benefit_data

 
    # Add the final price, premium rate and the credibility loading into the result by benefit level
    total_final_premium = 0
    for key in result.keys():
        benefit_name = key.lower()
        total_final_premium += result[key]["technical_premium"] 

    # Add total to result 
    total["technical_premium"] = total_final_premium 
    result["total"] = total

    return result





def arrondi_sup(x, n):
    facteur = 10 ** n
    return math.ceil(x * facteur) / facteur


def lambda_handler(event, context):
    ## Get lambda inputs
    total_premiums = event.get("total_premiums", {})
    details_premiums = event.get("details_premiums", {})
      ### result from the generic lambda ( calculation defined in the offer library)
    payment_method = event.get("payment_method", {})
    pooling = event.get("pooling", {})  ## -> add the calculation with pooling
    effective_date = datetime.strptime(event.get("effective_date"), "%Y-%m-%d").date()
    policy_period = event.get("policy_period", {})


    ##Calculation by catagory by plan 
    blocks =  [ "Inpatient", "Oupatient" , "Child Birth", "Dental"]
    premiums_line_by_line_block = []

    for index, entry in enumerate(details_premiums["1"]):   ## for the moment only offer_1
        total_sum = 0
        entry_key = f"Entry {index + 1}"
        premiums = {} 
        for block in blocks:
            if block in entry:
                block_sum = sum((entry[block].values()))
                premiums[block] = block_sum
                premiums["plan"]= entry['plan']
                premiums["exposure"]= entry['exposure']
                premiums["category"]= entry['category']
                total_sum += block_sum
                #premiums = { **premiums, **entry[block] }    
        
        premiums["total"] = total_sum
        premiums_line_by_line_block.append(premiums)
    group_cols = ["category", "plan"]
    blocks.append("exposure")
    df= pd.DataFrame( premiums_line_by_line_block)
    # Groupby multi-index
    grouped = df.groupby(group_cols)[blocks].sum()

    result = {}

    for plan in grouped.index.get_level_values("plan").unique():
        # filtre sur un plan donné
        sub = grouped.xs(plan, level="plan")
        
        # construit la structure demandée
        result[str(plan)] = {
            col: sub[col].to_dict()
            for col in blocks
    }







    offers_results_by_plan = copy.deepcopy(total_premiums['offers'])

    for offer_key, offer in total_premiums['offers'].items():

        # Object aggreagating results by benefit for each offer
        offers_results_by_benefit = {}
        offers_results_by_block= {}
        for plan_id, plan in offer['plans'].items():
                for block_id, block in plan['blocks'].items():
                    new_benefits = {}
                    for benefit_id, benefit in block['benefits'].items():
                        if benefit != {}:
                            new_benefit = copy.deepcopy(benefit)

                            ### Make the calculation here on new_benefit 

                            new_benefits[benefit_id] = copy.deepcopy(new_benefit)

                    offers_results_by_plan[offer_key]['plans'][plan_id]['blocks'][block_id]['benefits'] = (
                        copy.deepcopy(new_benefits)
                    )
        offers_results_by_benefit[offer_key] = extract_offer_benefits_results(offers_results_by_plan[offer_key], effective_date, policy_period)
        offers_results_by_block[offer_key] = extract_offer_blocks_results(offers_results_by_plan[offer_key], effective_date, policy_period)

    
    return {
        "result_by_benefit": offers_results_by_benefit,
        "result_by_block": offers_results_by_block , 
        "result_by_plan": offers_results_by_plan ,
        "result_by_plan_by_category": result  }