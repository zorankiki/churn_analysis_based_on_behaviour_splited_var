import pandas as pd

def get_variables_base_names(all_vars):
    all_vars_base_names = dict.fromkeys(all_vars)
    for var_ in all_vars:
        var_new_name = var_
        if var_ == 'QR.code.flyer.scans.total':
            var_new_name = 'other_non_contactless_menu_qr_flyer_scans'
        elif var_ == 'Consumer.job.listings.inquiry.total':
            var_new_name = 'consumer_job_listing_inquiries'
        elif 'total' in var_:
            var_new_name = var_[0:-6]

        if '.' in var_:
            var_new_name = var_new_name.replace('.', '_')

        var_new_name = var_new_name.lower()

        all_vars_base_names[var_] = var_new_name

    return all_vars_base_names

### all variables for the Cox models based on behaviour ###
posting_vars = ['Posts.on.facebook.total',\
'Changed.post.picture.total', \
'Changed.post.text.total', \
# 'Changed.picture.or.text.total', \
'Posts.disliked.total',\
'Posts.liked.total',\
'Posts.seen.total',\
'Flyer.posts.on.facebook.total',\
'Preview.page.views.facebook.total', \
'Preview.page.views.email.total', \
'Preview.page.views.twitter.total', \
'Number.of.requests.for.new.text.fragment.total', \
'Text.fragment.suggestion.applied.total']


inquiries_vars = ['private.parties.submissions.total',\
'Consumer.job.listings.inquiry.total',\
'catering.submissions.total',\
'Online.orders.total',\
'reservations.submissions.total',\
]

QR_scans_vars = ['QR.code.menu.scans.total',\
'QR.code.flyer.scans.total']

HubSpot_related_vars = ['Clicked.emails.total',\
'Opened.rewarding.stats.emails',\
'tickets.total']


SpotHopper_admin_vars = ['Added.events.manually.Edited.events.total',\
'Added.food.Edited.food.total',\
'Added.specials.Edited.specials.total',\
'Emails.sent.manually.Scheduled.emails.sent.total',\
'Visited.admin.total',\
'Visited.events.page.total',\
'Visited.food.page.total',\
'Visited.inquiries.pages.total',\
'Visited.special.page.total',\
'Visited.stats.page.total',\
'Downloaded.regular.flyers.total',\
'Downloaded.qrcode.flyers.total',\
'Visited.regular.flyers.page.total',\
'Visited.qrcode.flyers.page.total']

incentives_vars = ['Incentive.downloads',\
'Bday.club.downloads']

submissions_vars = ['feedback.submissions.total']

changed_inquiry_status_vars = ['R.Changed.inquiry.status.total',\
'C.Changed.inquiry.status.total',\
'PP.Changed.inquiry.status.total',\
'O.Changed.inquiry.status.total',\
'JL.Changed.inquiry.status.total']

List3 = ['instagram_process_New', 'instagram_process_Old',\
            'slider_balanced','slider_increase_revenue','slider_save_time',\
            'payment_method_ACH', 'payment_method_CC', 'payment_method_CHECK']


def main():
    all_vars = posting_vars + inquiries_vars + QR_scans_vars + HubSpot_related_vars + SpotHopper_admin_vars + \
    incentives_vars + submissions_vars + changed_inquiry_status_vars  #+ List3

    all_vars_base_names = get_variables_base_names(all_vars=all_vars)

    return (all_vars, all_vars_base_names)
