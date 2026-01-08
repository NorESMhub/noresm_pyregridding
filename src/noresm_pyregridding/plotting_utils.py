import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import xarray as xr
import math

from matplotlib.colors import LogNorm
from .misc_help_functions import get_unit_conversion_and_new_label

def make_3D_plot(bias,figname,yminv=None,ymaxv=None):

    dims = list(bias.dims)
    extra_dim = [d for d in dims if d not in ["lat", "lon"]][0]
    n = bias.sizes[extra_dim]
    labels = [f"{extra_dim}={i}" for i in range(n)]
    ncols = math.ceil(math.sqrt(n))
    nrows = math.ceil(n / ncols)
    fig, axs = plt.subplots(nrows, ncols, figsize=(4*ncols, 3*nrows), constrained_layout=True)
    axs = axs.flatten()
    ims = []
    fs=fig.suptitle(figname.split("/")[-1])
    cfs = fs.get_fontsize()
    fs.set_fontsize(cfs * 1.3)
    plotted_axes = []
    for i, ax in enumerate(axs, start=0):
        if i < n:       
            im =bias.isel({extra_dim: i}).plot.pcolormesh(ax=ax,vmin=yminv,vmax=ymaxv,add_colorbar=False,cmap="gist_earth")
            current_fs = ax.title.get_fontsize()   # get current font size
            ax.set_title(labels[i], fontsize=current_fs * 1.5)
            ax.set_xlabel('')
            ax.set_xticks([])
            ax.set_ylabel('')
            ax.set_xticklabels([])
            ax.set_yticklabels([])
            ims.append(im)
            plotted_axes.append(ax)
        else:
            fig.delaxes(ax)
    cbar=fig.colorbar(ims[0], ax=plotted_axes,location="bottom",fraction=0.04, pad=0.04)
    cbar.ax.tick_params(labelsize=16)             
    fignamefull=figname+'.png'
    fig.savefig(fignamefull,bbox_inches='tight')

                    
def make_bias_plot(bias,figname,yminv=None,ymaxv=None,cmap = 'gist_earth',ax = None, xlabel=None, logscale=False):
    # Use gist_earth for absolute maps

    print_to_file = False
    if ax is None:
        print_to_file = True
    else:
        shrink = 0.5

    if ax is None:
        plottype='singleplot'
    else:
        plottype='multiplot'
    print("in make bias plot",bias.name)

    dims = list(bias.dims)
    if(len(dims) == 3):
        bias_2d_plot=bias.sum(dim=bias.dims[0])
    else:
        bias_2d_plot = bias        
    if ax is None:
        print_to_file = True
        fig = plt.figure(figsize=(10, 5))
        ax = plt.axes(projection=ccrs.Robinson())
        print_to_file = True
        shrink = 0.7
    else:
        shrink = 0.5

    if xlabel is not None:
        shift, xlabel = get_unit_conversion_and_new_label(xlabel.split("[")[-1][:-1])
        bias_2d_plot = bias_2d_plot + shift

    try:
        if (yminv is None) or (ymaxv is None):
            if not logscale:
                im = bias_2d_plot.plot(ax=ax, transform=ccrs.PlateCarree(),cmap=cmap)
            else:
                bias_2d_plot = bias_2d_plot.where(bias_2d_plot > 0)
                im = bias_2d_plot.plot(ax=ax, transform=ccrs.PlateCarree(),cmap=cmap, norm = LogNorm())
        else:
            im = bias_2d_plot.plot(ax=ax, transform=ccrs.PlateCarree(),cmap=cmap, vmin=yminv, vmax=ymaxv)
        cb =  im.colorbar
        cb.remove()
        plt.colorbar(im, ax=ax, shrink=shrink)#fraction=0.046, pad=0.04

    except TypeError as err:
        print(f"Not able to produce plot due to {err}")
        ax.clear()
        
    ax.set_title('')
    ax.set_title(figname.split("/")[-1])

    if xlabel is None:
        ax.set_xlabel('')
    else:
        ax.set_xticks([])
        ax.set_xlabel(xlabel)
    ax.set_ylabel('')
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.coastlines()

    # Save 2D plot. 
    if print_to_file:
        fignamefull=figname+'.png'
        fig.savefig(fignamefull,bbox_inches='tight')

def make_bias_plot_latixy_longxy(bias,latixy, longxy, figname,yminv,ymaxv,cmap = 'RdYlBu_r', log_plot=False):
    # Use gist_earth for absolute maps
    fig = plt.figure(figsize=(10, 5))
    # Create a GeoAxes with the PlateCarree projection
    #ax = plt.axes(projection=ccrs.PlateCarree())
    
    ax = plt.axes(projection=ccrs.Robinson())
    
    # Plot the data on the map
    filled_c = ax.contourf(longxy, latixy, bias, cmap=cmap, transform=ccrs.PlateCarree(), vmin=yminv, vmax=ymaxv)
    ax.set_title('')
    ax.set_title(figname.split("/")[-1])
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.coastlines()
    fig.colorbar(filled_c, vmin=yminv, vmax=ymaxv)
    
    # Show the plot
    fignamefull=figname+'.png'
    plt.savefig(fignamefull,bbox_inches='tight')

