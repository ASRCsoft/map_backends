# getting netCDF files from kratos

# Requires a UAlbany user account with access to the RIT lulab folder

import paramiko, scp

sftpURL   =  'headnode7.rit.albany.edu'
sftpUser  =  '<ualbany username>'
sftpPass  =  '<ualbany password>'
data_dir = '/network/rit/lab/lulab/shengpo/source/IDEA-I_aerosolEntHR/products/NORTHEAST/AerosolEntHR/SNPP'

def get_file(conn, fname, dest='', errors=0):
    if errors > 5:
        raise Exception("couldn't connect to Kratos")
    try:
        conn.get(fname, dest)
    except SSHException: # if we get disconnected
        conn = scp.SCPClient(ssh.get_transport())
        get_file(conn, fname, dest, errors + 1)

ssh = paramiko.SSHClient()
# automatically add keys without requiring human intervention
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(sftpURL, username=sftpUser, password=sftpPass)

# get the available data folders
ftp = ssh.open_sftp()
folders = ftp.listdir(data_dir)

# set up the scp connection
kscp = scp.SCPClient(ssh.get_transport())
# kscp.get(data_dir + '/20180116/VIIRSaerosolEntHRS_traj_36hr_20180116.nc')
# kscp.get(traj_file)

# download the corresponding netcdf files
for folder in folders:
    traj_file = data_dir + '/' + folder + '/VIIRSaerosolEntHRS_traj_36hr_' + folder + '.nc'
    grid_file = data_dir + '/' + folder + '/VIIRSaerosolEntHRS_grid_36hr_' + folder + '.nc'
    for f in [traj_file, grid_file]:
        get_file(kscp, f, 'data/')
