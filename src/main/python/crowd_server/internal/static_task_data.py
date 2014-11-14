from django.shortcuts import render

task_context = {
    "cadbe13e-192c-49cc-9914-ab5f2eb1580b": [
        ["University of California <b>Statistics Department</b> Berkeley CA 94720 USA", 4], 
        ["<b>Mathematics Department</b> University of California Berkeley CA 94720 USA", 10]
    ],
    "efbd7578-13e0-410e-9567-cee87836c9b1": [
        ["Stanford <b>U.</b>", 4],
        ["<b>U.</b> C. Berkeley", 6]
    ],
    "69a6f7f1-c9a5-400a-9fb2-e6c6670a5bed": [
        ["Lawrence Berkeley National Laboratory 1 Cyclotron Road <b>Berkeley</b> CA 94720 USA", 3],
        ["University of California at <b>Berkeley</b> Berkeley CA 94720 USA", 7]
    ],
    "799237ed-a2ae-4c3e-bdeb-eceb1b0a45a6": [
        ["<b>Computer Science Division</b> University of California <b>Berkeley</b> CA", 18],
        ["International <b>Computer Science Institute Berkeley</b> CA USA", 4]
    ],
    "f7b1353e-6ef5-4e97-b278-224e92184cf6": [
        ["University of California <b>Statistics Department</b> Berkeley CA 94720 USA", 4], 
        ["University of California Berkeley <b>Department of Chemical Engineering</b> Berkeley CA 94720 USA", 4]
    ]
}

group_context = {"fields": ["affiliation", "count"]}

CONTEXT = {
    'group_context': group_context,
    'content': task_context,
    'task_id': 'bogus',
    'worker_id': 'bogus',
    'is_accepted': True,
    'assignment_id': 'bogus',
    'backend_submit_url': '',
    'frontend_submit_url': '',
    'base_template_name': 'internal/static_demo.html',
}

def render_static_template(request):
    return render(request, 'internal/er_demo.html', CONTEXT)
