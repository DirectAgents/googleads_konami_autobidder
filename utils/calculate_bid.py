
def calculate_new_bid(lh_bid_change: float, imp_hoh: float, lh_bid: float):
    new_bid = 0
    if 0 < lh_bid_change <= 1:
        if 0 > imp_hoh >= -1:
            new_bid = lh_bid + .5
        elif imp_hoh < -1:
            new_bid = lh_bid + 1
        else:
            new_bid = lh_bid
    elif lh_bid_change > 1:
        if 0 > imp_hoh >= -1:
            new_bid = lh_bid + .75
        elif imp_hoh < -1:
            new_bid = lh_bid + 1.25
        else:
            new_bid = lh_bid

    if 0 > lh_bid_change >= -1:
        if 0 < imp_hoh <= 1:
            new_bid = lh_bid - .5
        elif imp_hoh > 1:
            new_bid = lh_bid - 1
        else:
            new_bid = lh_bid
    elif lh_bid_change < -1:
        if 0 < imp_hoh <= 1:
            new_bid = lh_bid - .75
        elif imp_hoh > 1:
            new_bid = lh_bid - 1.25
        else:
            new_bid = lh_bid

    return new_bid


def calculate_new_mbid(bid_change: float, new_bid: float):
    new_mbid = 0.65
    if 0 < bid_change <= .5:
        new_bid += .25
        new_mbid += .05
    elif .5 < bid_change <= 1:
        new_bid += .5
        new_mbid += .07
    elif 1 < bid_change <= 1.5:
        new_bid += .75
        new_mbid += .1
    elif 1.5 < bid_change <= 2:
        new_bid += 1
        new_mbid += .15
    elif 2 < bid_change <= 2.5:
        new_bid += 1.25
        new_mbid += .2
    elif bid_change > 2.5:
        new_bid += 1.5
        new_mbid += .25
    elif 0 >= bid_change >= -.5:
        new_bid -= .25
        new_mbid -= .05
    elif -.5 > bid_change >= -1:
        new_bid -= .5
        new_mbid -= .07
    elif -1 > bid_change >= -1.5:
        new_bid -= .75
        new_mbid -= .1
    elif -1.5 > bid_change >= -2:
        new_bid -= 1
        new_mbid -= .15
    elif -2 > bid_change >= -2.5:
        new_bid -= 1.25
        new_mbid -= .2
    elif bid_change < -2.5:
        new_bid -= 1.5
        new_mbid -= .5

    return new_mbid
