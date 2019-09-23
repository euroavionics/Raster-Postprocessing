# -*- coding: utf-8 -*-
'''
# Script to transform PNGs in en7 data
# Change lines 73-74 to change resolution of TIFF files (OPTIONALLY)
# Change lines 165-168 to adapt the name and scale odf the dataset (MANDATORY)
# Run like: python ./raster_postprocessing.py
'''

from __future__ import division
import sys, os
import subprocess
from time import strftime
from psutil import virtual_memory
import datetime
import shutil
import psycopg2
#import database as db

try:
    from osgeo import gdal #ogr, osr
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
#Print starting time to control
time1 = datetime.datetime.now()
print time1
class PostProcessing():
    
    def __init__(self, name, group, png, category="topo", iso=None):

# =============================================================================
#          Set variables need for the script to work.
# =============================================================================
        self.iso = name[:3]
        self.name = name
        self.group = group
        self.category = category
        self.published = str(strftime("%Y-%m"))        
        self.png = png
        self.shp = None
        
        self.homedir = os.path.expanduser('~')
        #print self.homedir
        self.mainfolder = os.path.abspath(os.path.join(png, os.pardir, os.pardir))
        self.workingdir = os.path.abspath(os.path.join(self.homedir,'server/maps-02/temp_mapcruncher', self.iso))
        print '\n' + 'mainfolder: ' + str(self.mainfolder) + '\n'
        print 'working directory: ' + str(self.workingdir) + '\n'
        
# =============================================================================
#         Checking the number of files and if they are consistent
# =============================================================================
        if os.path.isdir(self.png):
            #os.path.isdir(os.path.join(self.png))
            self.num_files = int(len([names for names in os.listdir(self.png) if os.path.isfile(os.path.join(self.png, names)) and names[-3:]!='.db']))
            print self.num_files
            if self.num_files == 0:
                sys.exit('ERROR: There are no files to process.')
            elif (self.num_files %2) != 0:
                sys.exit('ERROR: One png or pgw is missing in the geo folder')
            else:
                self.num_files = int(self.num_files/2)
                print 'The total number of files to process is : ' + str(self.num_files)
        else:
            sys.exit('The mainfolder directory do not exist, check your png folder path.')

# =============================================================================
#         Creating the folders need to process the images and the final
#         destination folder
# =============================================================================
        if continent != None:
            self.rgba = self.create_folder(os.path.join(self.workingdir, "rgba_" + continent))
            self.wgs84 = self.create_folder(os.path.join(self.workingdir, "wgs84_" + continent))
        else:
            self.rgba = self.create_folder(os.path.join(self.workingdir, "rgba"))
            self.wgs84 = self.create_folder(os.path.join(self.workingdir, "wgs84"))
        self.converted = self.create_folder(os.path.join(self.workingdir, name))
        self.en7 = self.create_folder(os.path.join(self.mainfolder, "en7", 'raster'))
        
        self.cruncher = "~/server/Eanserver18/raster/tmp/BR/"

# =============================================================================
#         if self.iso:
#             try:
#                 print 'I have an ISO'
#                 self.path = self.create_folder(os.path.join(self.mainfolder,'tools', 'shp'))
#                 self.shp = self.get_crop_polygon(self, self.png)
#             except:
#                 self.shp = None
#         else:
#             self.shp = None
# =============================================================================


    @staticmethod    
    def create_folder(path):
        if not os.path.exists(path):
            print ("----- CREATE FOLDER: %s -----" % (path))
            os.makedirs(path)
        else:
            print ("----- THE FOLDER %s EXISTS ------" % (path))
        return path    

    @staticmethod
    def get_rastersize(rgba_file):  
        gtif = gdal.Open(rgba_file)
        if gtif is None:
            print 'Unable to open tif' +'\n'
            sys.exit(1)
        x = gtif.RasterXSize
        y = gtif.RasterYSize
        
        # For 2/3 pixel-size
        # x = int(math.floor(gtif.RasterXSize*(3/3.0)))
        # y = int(math.floor(gtif.RasterYSize*(3/3.0)))
        return x, y

#Function created to get the intersecting tiles among the border of a country
    def get_kachel_liste(self):
        self.kachel_liste = []
        cur = self.sql_get_kachel_country()
        for row in cur:
            self.kachel_liste.append(row)
        print self.kachel_liste
        kachel_anzahl = len(self.kachel_liste)
        print kachel_anzahl, 'tiles intersects the border'

#This funtion builds the query and returns the cursor with the information
    def sql_get_kachel_country(self):
        self.connect_db()
        if group == '50k':
            table = 'kachel_1_25grad'
        elif group == '100k':
            table = 'kachel_2_5grad'
        else:
            print 'group is wrong'
            sys.exit('The group you write is not correct, choose between 100k and 50k')
        sql = " SELECT k.name AS name"\
        "FROM country.%s AS k,"\
        "country.country AS country"\
        "WHERE ST_Intersects(st_boundary(country.geom_boundary_buffer), k.geom)"\
        "AND country.iso = '%s'" % (table, self.iso)"\
        cur = self.executereturn(sql)
        return cur
        self.closeDBconnect()

#Two functions to connect / disconnec from the database
    def connect_db(self):
#        import psycopg2
        dbnamePG = "test"
        userPG = "creation_center" #"osm" # 
        hostPG = "10.49.20.78" #Workstaion # "localhost" #for local
        portPG = "5432"
        pwPG = "openstreetmap" #"test" #
        global connection, cur
        connection_str = "dbname='%s' host='%s' user='%s' password='%s'" % (dbnamePG,hostPG,userPG,pwPG)
        connection = psycopg2.connect(connection_str)
        print ("Verbindung zu Datenbank " + str(connection) + " erfolgreich aufgebaut")
        cur = connection.cursor()
    def closeDBconnect(self):
        print ('close DB-Connection ' + str(connection))
        cur.close()
        connection.close()

#Function to execute an SQL query, call allways after connect_db, don't forget
#to close the connection after
    def executereturn(self,sql_query):
        print ("Start sql_query:\n%s" % (sql_query))
        cur.execute(sql_query)
        return cur

# =============================================================================
#     Gdaltranslate. Transform the png into GTiff with LZW compression,
#     expand to rgba, with the resampling method as cubic, and predictor 
#     method 2 the predictor saves the difference in value with the neighbour 
#     pixel instead of the value itself.
# =============================================================================
    def gdaltranslate(self):
        xarg = "find {png_folder} -iname *.png -print0 | xargs -0 -P4 -I{src} bash -c ".format(png_folder=self.png, src='{}')
        gdaltranslate = "'gdal_translate -q -of GTiff  -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 \
        -co {Tiled} -co compress=LZW -expand {expand} \
        -r cubic -co PREDICTOR=2 {src} {dst}'".format(Tiled='"Tiled=YES"', expand='"rgba"',
        src='{}', dst=self.rgba+"/$(basename {})_rgba.tif")
        gdaltranslate_command = xarg + gdaltranslate
        print gdaltranslate_command +'\n'
        sp1 = subprocess.Popen([gdaltranslate_command], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        sp1.wait()

# =============================================================================
#     Do the query to the data base with the iso to get back and store the
#     resultant shapefile to use later in the function gdalwarp(). Data is
#     saved in the main folder.
# =============================================================================
    def get_crop_polygon(self, path):
        path = os.path.dirname(path)
        path_shp = os.path.join(os.path.dirname(path),'tools','shp')
        self.shp = self.create_folder(path_shp)
        print path_shp
        if len(os.listdir(path_shp)) == 0:
            #self.iso = name[:3]
            # Export-Command - ogr2ogr // pgsql2shp 
            export_sql = "SELECT geom_boundary_buffer as geom from country.country WHERE iso='{}'".format(self.iso) #SQL modifyed to allow acces to buffer table.
            print export_sql +'\n'
            export_command = 'ogr2ogr -f "ESRI Shapefile" "{0}" PG:"host= 10.49.20.78 user=osm dbname=ngmaps password=test" -sql "{1}"'.format(os.path.join(path_shp,"cutline_"+self.iso+".shp"), export_sql)
            #The pqsql2shp command was discarde in favour of ogr2ogr because the pgsql2shp didn't worked as expected. It has produced the shapefiles but they were empty.
            #export_command = 'sudo pgsql2shp -f "{0}" -h 10.49.20.78 -u osm -P test ngmaps "{1}"'.format(os.path.join(path,"cutline_"+self.iso), export_sql)
            print export_command +'\n'
            sp1 = subprocess.Popen([export_command], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            sp1.wait()
        else:
            print 'The shapefile was already there'

        pathshp, dirs, files = next(os.walk(path_shp))
        if len(files) > 3:
            self.shp = os.path.join(path_shp,"cutline_"+self.iso+".shp")
        else:
            sys.exit('There was a problem creating the shp, check if any app is using the shp file')
        #print self.shp +'\n'
        #return path, shp

# =============================================================================
#     Make the count of the pending tiles, and as the gdalwarp command needs
#     to be applied one by one, use the list of the tiff directory and call
#     the function gdalwarp(), pass the pixel ratio and the shapefile
#     as well.
# =============================================================================
    def warp_main(self):
        i=0
        for tif in os.listdir(self.rgba):
            if tif[-4:] == '.tif':
                i += 1
                percentages = int((i*100)/self.num_files)
                print '\n' + tif
                print 'Processing image {0} from a total of {1}'.format(i, self.num_files)
                wgs84 = os.path.join(self.wgs84,tif[:-4]+"_wgs84.tif")
                tif = os.path.join(self.rgba,tif)
                x, y = self.get_rastersize(tif)
                tifs_border = []
                tifs_not_border = []
                if tif[:7] in self.kachel_liste:
                    tifs_border.append()
                else:
                    tifs_not_border.append()
                print tifs_border
                print tifs_not_border
                #self.gdalwarp(x, y, tif, wgs84, self.shp)
                #print '\nPercentage completed: {0}%'.format(percentages)

# =============================================================================
#     Get the arguments from the function warp_main() and use them to apply the
#     gdalwarp command for every tiff file. The gdalwarp does the transformation
#     from web-mercator to wgs84, and cut the tiffs with the shape file, if
#     exists, all other parameters are either same as in gdaltranslate or are
#     performance parameters (refer to online documentation)
# =============================================================================
    @staticmethod
    def gdalwarp(x, y, rgba, wgs84, shp=None):
        #shp = '/home/EA/cc/server/maps-02/raster_rev003/fac/leo/20190107/tools/leo_fac.shp'
        #print "shp = "+str(shp) +'\n'
        cutline = "" if shp is None else "-cutline '{}'".format(shp)#+" -crop_to_cutline"
        if shp:
            print "cutline =" +str(cutline) +'\n'
        else:
            pass
        #print "cutline =" +str(cutline) +'\n'
        #To calculate the available memory and use only 1/2 allowing performance and usability of the computer.
        available_Mb = max(1024, (virtual_memory().available / (1024 ** 2)) / 2)
        gdalwarp_command = 'gdalwarp -q -s_srs EPSG:3857 -t_srs EPSG:4326 \
        -of GTiff -r cubic -co "Tiled=YES" -co compress=LZW -co PREDICTOR=2 \
        {cutline} -wm {wm} -multi -ts {x_pix} {y_pix} \
        -wo "NUM_THREADS=4" {src} {dst} --config GDAL_CACHEMAX {wm} \n'.format(cutline=cutline, wm=available_Mb,
        x_pix=x, y_pix=y, src=rgba, dst=wgs84)

        #Changed parameters for gdalwarp: -co "DISCARD_LSB=4" This parameter is discarded because it's compression algorithm introduces an extra value around the croped line
        #Changed parameters for gdalwarp: -wm 1024 Increased Working memory to half of the resources, improves performance without blocking computer with parallel jobs.
        #Changed parameters for gdalwarp: Added -crop_to_cutline as cutline (only a mask in the raster without modifying it) working alone will keep the full extent and data of the original raster
        print gdalwarp_command +'\n'
        #sp1 = subprocess.Popen([gdalwarp_command], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        #sp1.wait()

# =============================================================================
#     Build the vrt file of all GTiff, here is important to keep the resolution
#     to the maximum possible avoiding the loose of quality, and to tell the 
#     no data to have the value 0 (transparent)
# =============================================================================
    def gdalvrt(self):
        vrt_command = "gdalbuildvrt {name}.vrt -resolution highest -srcnodata 0 {wgs84}/*.tif".format(name=os.path.join(self.wgs84, self.name), wgs84=self.wgs84)
        print '\n'+vrt_command +'\n'
        sp1 = subprocess.Popen([vrt_command], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        sp1.wait()

# =============================================================================
#     Here the conversion to the EN7 system is done, the map cruncher is a 
#     'in house' made software to allow visualization in the EN7 softwares
#     the cruncher command below is really 3 different commands written together
#     the first one gets the vrt created with the function gdalvrt() and creates
#     a pck intermediate file, the next one takes this pck and generate the final
#     EN7 pck inside the right folder (those pck have different structure), and
#     the last command extracts the zoom level files from the pck and stores them
#     in the folder.
#     After the mapcruncher is done, the first pck shall be deleted, as this is 
#     not anymore usefull for this process, once the file is deleted the final
#     data is copied to the final en7 folder and there the md5 file is generated.
# =============================================================================
    def mapcruncher(self):
        cruncher_command = "{cruncher}/mapcruncher -n {name} -g {group} \
        -c {category} -p {publish} {vrt}.vrt {name_pack} && date && echo ' ' \
         && do_refurbish-raster-map {name_pack} {name_pack_converted} \
         && extract-map.def-zl.def {name_pack_converted} {converted}".format(
         cruncher=self.cruncher, name=self.name, group=self.group,
         category=self.category, publish=self.published, vrt=os.path.join(self.wgs84, self.name),
         name_pack=os.path.join(self.workingdir,self.name+".pck"), name_pack_converted=os.path.join(
                 self.converted,name+".pck"), converted=self.converted)
        print cruncher_command +'\n'
        sp1 = subprocess.Popen([cruncher_command], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        sp1.wait()
        print 'Map crucher done'
        time2 = datetime.datetime.now()
        print time2
        time = time2 - time1
        print 'Elapsed time = '+str(time)
        for file in os.listdir(self.workingdir):
            if 'pck' in file and name in file:
                print 'Removing pack file level 0: \n' + os.path.join(self.workingdir,file)
                os.remove(os.path.join(self.workingdir,file))
        #Move the result to maps01 folder before creating the md5 sum to avoid problems:        
        print 'Copying the data to main folder'
        movecommand = 'cp -i -R -v {0} {1}'.format(os.path.join(self.workingdir,name),self.en7) #-i ask before overwritte
        print movecommand +'\n'
        sp1 = subprocess.Popen([movecommand], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        sp1.wait()

        #Generate the md5sum in the destination folder
        md5sum_command = 'cd {0} && md5sum *.pck* > {1}'.format(os.path.join(self.en7,name), name+'.md5')
        print md5sum_command +'\n'
        sp1 = subprocess.Popen([md5sum_command], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        sp1.wait()

# =============================================================================
#     This function is usefull to create the en5 data, for this we do an inverse
#     process from the mapcruncher function, the first command takes the pck
#     and transform the internal structure to the en5 format and compress it,
#     the second command compress it a second time a little bit more.
#     If data is bigger than 2Gb (size file limit from en5 disks) we split the
#     data in junks of 1 Gb
# =============================================================================
    def en5daten(self):
        pckcommand = 'pack2tar {0} | gzip > {1} '.format(os.path.join(self.converted,name+'.pck'),
                                os.path.join(self.en7,name+'.tar.gz'))
        print pckcommand + '\n'
        sp1 = subprocess.Popen([pckcommand], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        sp1.wait()
        pck_size = (os.path.getsize(os.path.join(self.en7,name+'.tar.gz')))/1024.**3 #Get file size in Gb
        if pck_size >= 2:
            print 'File size is bigger than 2 Gb, the tar file shall be splitted'
            split_command = 'split --numeric-suffixes --suffix-length=2 --bytes=1G {0} {1}'.format(
                    os.path.join(self.en7,name+'.tar.gz'),os.path.join(self.en7,name+'.tar.gz.'))
            print split_command +'\n'
            sp1 = subprocess.Popen([split_command], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            sp1.wait()

# =============================================================================
#     Small function to delete the intermediate data generated by gdaltranslate
#     and gdalwarp in the previous steps. The user will be asked before delete
# =============================================================================
    def del_data(self):
        self.answ = raw_input('Do yo want to delete the RGBA and WGS84 folders? (y/n)')
        if self.answ == 'y' or self.answ == 'yes':
            print 'Removing unnecesary data: {0} \nand \n{1}'.format(self.rgba, self.wgs84)
            try:
                shutil.rmtree(self.rgba)
                shutil.rmtree(self.wgs84)
            except:
                print 'Could not delete files'
        elif self.answ == 'n' or self.answ == 'no':
            print 'Did not delete anything, please delete files manually.'
        else:
            print 'Invalid entry, did not delete any data.'

scale = "50k"
png = r"R:\packages\europe\esp\eng\20190715_rev003\geo\50k" #Copy the path from "server"
name = "esp_50k_eng"
group = "50k"
continent = "50k" #"50k"     # This is just to add a surname to the subfolders need for the 
                            # parallel processing of diferent scripts (max of 2 processes).
homedir = os.path.expanduser('~')
png = os.path.join(homedir, png.lstrip(os.path.sep))

# main
m = PostProcessing(name, group, png)
m.get_kachel_liste()
m.gdaltranslate()
m.get_crop_polygon(png)
m.warp_main()
m.gdalvrt()
m.mapcruncher()
#m.en5daten()
m.del_data()


time2 = datetime.datetime.now()
print time2
print time2 - time1

